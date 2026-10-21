package dev.conductor.core;

import java.util.*;

/**
 * The workflow state machine as pure functions over a mutable execution projection.
 * Storage backends own copying, persistence, and concurrency; this class owns semantics.
 * Callers pass a private copy: every method may mutate {@code run}.
 */
public final class Workflow {
    private Workflow() {}
    public static final Set<String> TERMINAL=Set.of("SUCCEEDED","FAILED","COMPENSATED","COMPENSATION_FAILED","CANCELLED");
    private static final Set<String> ACTIONS=Set.of("echo","ledger","sleep","fail","wait","approval");
    private static final Set<String> UNDO_ACTIONS=Set.of("echo","ledger","fail");
    private static final java.util.regex.Pattern STEP_ID=java.util.regex.Pattern.compile("[a-zA-Z][a-zA-Z0-9_-]{0,63}");

    /** Result of a claim attempt on a claimable task. {@code claim} is null when attempts are exhausted. */
    public record ClaimResult(String event,Claim claim) {}

    public static boolean terminal(String status){return TERMINAL.contains(status);}
    /** {@code -Dconductor.deepCopyTransitions=true} restores whole-run deep copies, for A/B comparison. */
    private static final boolean DEEP_COPY_TRANSITIONS=Boolean.getBoolean("conductor.deepCopyTransitions");
    /**
     * The copy a transition mutates. The run map, the task map and every task map are fresh; nested
     * values (definition, step specs, results, signals) are shared, because no transition mutates
     * them in place. Far cheaper than a deep copy, and just as isolating for copy-on-write commits.
     */
    public static Map<String,Object> mutableCopy(Map<String,Object> run) {
        if(DEEP_COPY_TRANSITIONS)return Json.copy(run);
        Map<String,Object> copy=new LinkedHashMap<>(run);
        Map<String,Object> source=tasks(run),tasks=new LinkedHashMap<>(source.size()*2);
        for(var e:source.entrySet())tasks.put(e.getKey(),new LinkedHashMap<>(Json.obj(e.getValue())));
        copy.put("tasks",tasks);
        return copy;
    }
    public static Map<String,Object> tasks(Map<String,Object> run){return Json.obj(run.get("tasks"));}
    public static String status(Map<String,Object> m){return Json.str(m,"status","");}

    public static void validate(Map<String,Object> def) {
        String name=Json.str(def,"name",""),version=Json.str(def,"version","");
        if(name.isBlank()||version.isBlank())throw new IllegalArgumentException("name and version are required");
        List<Object> steps=Json.arr(def.get("steps"));
        if(steps.isEmpty()||steps.size()>100)throw new IllegalArgumentException("Use 1..100 steps");
        Map<String,Map<String,Object>> byId=new LinkedHashMap<>();
        for(Object s:steps) {
            Map<String,Object> step=Json.obj(s);String id=Json.str(step,"id","");
            if(!STEP_ID.matcher(id).matches()||byId.put(id,step)!=null)
                throw new IllegalArgumentException("Invalid or duplicate step id");
            String action=Json.str(step,"action","echo");
            if(!ACTIONS.contains(action))throw new IllegalArgumentException("Unknown action "+action);
            String undo=Json.str(step,"compensate","");
            if(!undo.isEmpty()&&!UNDO_ACTIONS.contains(undo))throw new IllegalArgumentException("Invalid compensation action");
            bounded(step,"maxAttempts",1,20,3); bounded(step,"backoffMs",0,3_600_000,100);
            bounded(step,"delayMs",0,31_536_000_000L,0); bounded(step,"durationMs",0,60_000,0);
            bounded(step,"failUntil",0,20,0); bounded(step,"latencyMs",0,60_000,0);
        }
        // Kahn-style layering: each pass must resolve at least one step or the graph has a cycle.
        Set<String> visited=new HashSet<>();
        while(visited.size()<steps.size()) {
            int before=visited.size();
            for(var e:byId.entrySet()) {
                List<Object> deps=Json.arr(e.getValue().getOrDefault("dependsOn",List.of()));
                for(Object dep:deps)if(!(dep instanceof String)||!byId.containsKey(dep))
                    throw new IllegalArgumentException("Unknown dependency");
                if(visited.containsAll(deps))visited.add(e.getKey());
            }
            if(visited.size()==before)throw new IllegalArgumentException("Workflow contains a cycle");
        }
    }
    private static void bounded(Map<String,Object> m,String key,long min,long max,long fallback){
        long n=Json.num(m,key,fallback);if(n<min||n>max)throw new IllegalArgumentException(key+" out of range");
    }
    public static void requireKey(String key) {
        if(key==null||key.isBlank()||key.length()>128)throw new IllegalArgumentException("Idempotency-Key required (1..128 chars)");
    }

    /** A new, already-advanced execution projection. The definition must be validated and owned by the caller. */
    public static Map<String,Object> newRun(Map<String,Object> definition,String key,String id,long now) {
        Map<String,Object> run=new LinkedHashMap<>();
        run.put("id",id);run.put("requestKey",key);run.put("definition",Json.copy(definition));
        run.put("createdAt",now);run.put("status","RUNNING");run.put("signals",new ArrayList<>());
        Map<String,Object> tasks=new LinkedHashMap<>();
        for(Object item:Json.arr(definition.get("steps"))) {
            Map<String,Object> spec=Json.obj(item);
            tasks.put(Json.str(spec,"id",""),newTask(Json.copy(spec),false));
        }
        run.put("tasks",tasks);advance(run,now);run.put("updatedAt",now);
        return run;
    }
    private static Map<String,Object> newTask(Map<String,Object> spec,boolean compensation) {
        Map<String,Object> t=new LinkedHashMap<>();
        t.put("spec",spec);t.put("status","PENDING");t.put("attempt",0L);t.put("fence",0L);
        if(compensation)t.put("compensation",true);
        return t;
    }

    public static void advance(Map<String,Object> run,long now) {
        Map<String,Object> ts=tasks(run);
        if(status(run).equals("RUNNING")) {
            boolean failed=false;
            for(Object v:ts.values())if(status(Json.obj(v)).equals("FAILED")){failed=true;break;}
            if(failed) {
                run.put("status","FAILING");
                for(Object v:ts.values()) {
                    Map<String,Object> t=Json.obj(v);String s=status(t);
                    if(!s.equals("RUNNING")&&!s.equals("SUCCEEDED")&&!s.equals("FAILED"))t.put("status","CANCELLED");
                }
            }
        }
        if(status(run).equals("FAILING")) {
            boolean draining=false;
            for(Object v:ts.values())if(status(Json.obj(v)).equals("RUNNING")){draining=true;break;}
            if(!draining)beginCompensation(run,ts);
        }
        String runStatus=status(run);
        boolean undo=runStatus.equals("COMPENSATING");
        if(!undo&&!runStatus.equals("RUNNING"))return;
        if(undo)for(Object v:ts.values()) {
            Map<String,Object> t=Json.obj(v);
            if(Boolean.TRUE.equals(t.get("compensation"))&&status(t).equals("FAILED")){run.put("status","COMPENSATION_FAILED");return;}
        }
        boolean allSucceeded=true;
        for(Object v:ts.values()) {
            Map<String,Object> t=Json.obj(v);
            if(Boolean.TRUE.equals(t.get("compensation"))!=undo)continue;
            if(status(t).equals("PENDING")) {
                Map<String,Object> spec=Json.obj(t.get("spec"));
                boolean ready=true;
                for(Object dep:Json.arr(spec.getOrDefault("dependsOn",List.of())))
                    if(!status(Json.obj(ts.get(dep))).equals("SUCCEEDED")){ready=false;break;}
                if(ready) {
                    t.put("readyAt",now+Json.num(spec,"delayMs",0));
                    t.put("status",Json.str(spec,"action","echo").equals("approval")?"WAITING":"READY");
                }
            }
            if(!status(t).equals("SUCCEEDED"))allSucceeded=false;
        }
        if(allSucceeded)run.put("status",undo?"COMPENSATED":"SUCCEEDED");
    }
    private static void beginCompensation(Map<String,Object> run,Map<String,Object> ts) {
        List<String> succeeded=new ArrayList<>();
        for(var e:ts.entrySet())if(status(Json.obj(e.getValue())).equals("SUCCEEDED"))succeeded.add(e.getKey());
        // Completion order is a topological order for successful DAG nodes.
        succeeded.sort(Comparator.comparingLong(id->-Json.num(Json.obj(ts.get(id)),"completedSequence",0)));
        String previous=null;
        for(String id:succeeded) {
            Map<String,Object> original=Json.obj(Json.obj(ts.get(id)).get("spec"));
            String undo=Json.str(original,"compensate","");
            if(undo.isEmpty())continue;
            String cid="$undo:"+id;
            Map<String,Object> spec=new LinkedHashMap<>();
            spec.put("id",cid);spec.put("action",undo);spec.put("value",Map.of("undo",id));
            spec.put("dependsOn",previous==null?List.of():List.of(previous));
            ts.put(cid,newTask(spec,true));previous=cid;
        }
        run.put("status",previous==null?"FAILED":"COMPENSATING");
    }

    /** When a READY task becomes due, or a RUNNING lease expires; {@link Long#MAX_VALUE} otherwise. */
    public static long dueAt(Map<String,Object> task) {
        return switch(status(task)) {
            case "READY" -> Json.num(task,"readyAt",0);
            case "RUNNING" -> Json.num(task,"leaseUntil",0);
            default -> Long.MAX_VALUE;
        };
    }
    public static boolean claimable(Map<String,Object> task,long now){return dueAt(task)<=now;}

    /** Claims a task for which {@link #claimable} holds. */
    public static ClaimResult claim(Map<String,Object> run,String taskId,String owner,long now,long leaseMs) {
        if(owner==null||owner.isBlank()||leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        Map<String,Object> t=Json.obj(tasks(run).get(taskId));
        boolean expired=status(t).equals("RUNNING");
        long attempt=Json.num(t,"attempt",0)+1,fence=Json.num(t,"fence",0)+1;
        Map<String,Object> spec=Json.obj(t.get("spec"));
        if(attempt>Json.num(spec,"maxAttempts",3)) {
            t.put("status","FAILED");t.put("error","Attempts exhausted after lease loss");
            advance(run,now);run.put("updatedAt",now);
            return new ClaimResult("AttemptsExhausted",null);
        }
        t.put("status","RUNNING");t.put("attempt",attempt);t.put("fence",fence);
        t.put("owner",owner);t.put("leaseUntil",now+leaseMs);t.put("claimedAt",now);run.put("updatedAt",now);
        String id=Json.str(run,"id","");
        return new ClaimResult(expired?"LeaseReclaimed":"TaskClaimed",
                new Claim(id,taskId,owner,fence,attempt,id+"/"+taskId,Json.copy(spec),Json.num(t,"readyAt",now)));
    }
    public static boolean valid(Map<String,Object> run,Claim c,long now) {
        if(run==null||terminal(status(run)))return false;
        Object value=tasks(run).get(c.task());if(value==null)return false;
        Map<String,Object> t=Json.obj(value);
        return status(t).equals("RUNNING") && Json.num(t,"fence",0)==c.fence()
                && Json.str(t,"owner","").equals(c.owner()) && Json.num(t,"leaseUntil",0)>now;
    }
    /** Requires {@link #valid}. */
    public static void heartbeat(Map<String,Object> run,Claim c,long now,long leaseMs) {
        if(leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        Json.obj(tasks(run).get(c.task())).put("leaseUntil",now+leaseMs);run.put("updatedAt",now);
    }
    /** Requires {@link #valid}. {@code order} is a per-execution monotonically increasing completion stamp. */
    public static String complete(Map<String,Object> run,Claim c,Object result,String error,long now,long order) {
        Map<String,Object> t=Json.obj(tasks(run).get(c.task())),spec=Json.obj(t.get("spec"));
        if(error==null) {
            t.put("status","SUCCEEDED");t.put("result",result);t.put("completedSequence",order);t.put("completedAt",now);
        } else {
            t.put("error",error);
            if(c.attempt()<Json.num(spec,"maxAttempts",3) && !status(run).equals("FAILING")) {
                t.put("status","READY");
                long delay=Math.min(3_600_000,Json.num(spec,"backoffMs",100)*(1L<<(c.attempt()-1)));
                t.put("readyAt",now+delay);
            } else t.put("status","FAILED");
        }
        advance(run,now);run.put("updatedAt",now);
        return error==null?"TaskSucceeded":"TaskFailed";
    }
    public static void signal(Map<String,Object> run,String step,long now) {
        if(terminal(status(run)))throw new IllegalArgumentException("Execution is terminal");
        Object value=tasks(run).get(step);if(value==null)throw new IllegalArgumentException("Unknown step");
        Map<String,Object> t=Json.obj(value);
        if(!status(t).equals("WAITING"))throw new IllegalArgumentException("Step is not waiting");
        t.put("status","READY");t.put("readyAt",now);run.put("updatedAt",now);
    }
    /** Returns false when the execution is already terminal (no change). */
    public static boolean cancel(Map<String,Object> run,long now) {
        if(terminal(status(run)))return false;
        run.put("status","CANCELLED");
        for(Object v:tasks(run).values()) {
            Map<String,Object> t=Json.obj(v);
            if(!status(t).equals("SUCCEEDED"))t.put("status","CANCELLED");
        }
        run.put("updatedAt",now);
        return true;
    }
    public static Map<String,Object> summary(Map<String,Object> run) {
        Map<String,Object> s=new LinkedHashMap<>();
        s.put("id",run.get("id"));s.put("status",run.get("status"));s.put("createdAt",run.get("createdAt"));
        s.put("updatedAt",run.get("updatedAt"));s.put("name",Json.obj(run.get("definition")).get("name"));
        return s;
    }
}
