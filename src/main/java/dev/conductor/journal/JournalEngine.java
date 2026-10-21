package dev.conductor.journal;

import dev.conductor.core.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Single-writer durable transitions with copy-on-write projections and group commit.
 *
 * <p>The WAL holds a full projection when an execution starts and a field-level {@link Patch} for
 * every later transition; replay applies patches to rebuild identical projections. (Records written
 * by earlier versions, full projections throughout, still replay.)
 *
 * <p>Writer-thread state ({@code runs}, the due heap) may hold speculative transitions until the
 * batch flushes. Readers only see {@code committed}, which is published after {@code force(true)},
 * so reads never queue behind the writer yet never observe undurable state. Projections are never
 * mutated after commit, which makes publishing a reference safe.
 */
public final class JournalEngine implements LocalEngine {
    private record Due(long at,long order,String execution,String task) {}
    /** A transition awaiting flush: the WAL record and the full projection readers will see. */
    private record Pending(Map<String,Object> record,Map<String,Object> run,long sequence) {}
    /** {@code -Dconductor.fullProjectionRecords=true} writes whole projections (v0.2 format), for A/B. */
    private static final boolean FULL_RECORDS=Boolean.getBoolean("conductor.fullProjectionRecords");

    // Writer-thread (and replay) state.
    private final Map<String,Map<String,Object>> runs=new HashMap<>();
    private final Map<String,String> requests=new HashMap<>();
    private final PriorityQueue<Due> due=new PriorityQueue<>(Comparator.comparingLong(Due::at).thenComparingLong(Due::order));
    private final Map<String,Long> indexed=new HashMap<>();
    private final List<Pending> pending=new ArrayList<>();
    /** Workers parked in {@link #claimOrWait}; served by the writer in the batch that makes work due. */
    private record Idle(String owner,long leaseMs,CompletableFuture<Claim> grant) {}
    private final ArrayDeque<Idle> idle=new ArrayDeque<>();
    /** {@code -Dconductor.directGrant=false}: wake-then-claim instead (one more flush per idle claim), for A/B. */
    private static final boolean DIRECT_GRANT=!"false".equals(System.getProperty("conductor.directGrant"));
    private long sequence,dueOrder;

    // Committed, concurrently readable state.
    private final ConcurrentHashMap<String,Map<String,Object>> committed=new ConcurrentHashMap<>();
    private final List<String> order=new ArrayList<>();
    private final Map<String,Long> statusCounts=new TreeMap<>();
    private volatile long committedSequence;
    private volatile long nextDue=Long.MAX_VALUE;
    private final WorkSignal signal;
    private final java.util.function.Supplier<String> ids;
    private final ConcurrentHashMap<String,List<CompletableFuture<Void>>> terminalWaiters=new ConcurrentHashMap<>();

    private final Journal journal;
    private final DurableDispatcher dispatcher;

    public JournalEngine(Path dir)throws IOException {this(dir,new WorkSignal(),()->UUID.randomUUID().toString(),"engine-writer");}
    /** A shard of a partitioned journal: shares the work signal; {@code ids} must route back to this shard. */
    JournalEngine(Path dir,WorkSignal signal,java.util.function.Supplier<String> ids,String writerName)throws IOException {
        this.signal=signal;this.ids=ids;
        journal=new Journal(dir.resolve("events.wal"),this::replay);
        nextDue=due.isEmpty()?Long.MAX_VALUE:due.peek().at();
        dispatcher=new DurableDispatcher(writerName,new DurableDispatcher.Log() {
            public DurableDispatcher.Staged stage()throws IOException{return stageCommand();}
            public void force(byte[] frames,int records)throws IOException{journal.force(frames,records);}
        });
    }

    @Override public String start(Map<String,Object> definition,String key,long now)throws IOException {
        Map<String,Object> owned=Json.copy(definition);
        return dispatcher.call(()->doStart(owned,key,now));
    }
    @Override public Claim claim(String owner,long now,long leaseMs)throws IOException {
        if(owner==null||owner.isBlank()||leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        // Lock-free fast path: nothing durable is due yet, so skip the writer round trip.
        if(now<nextDue)return null;
        return dispatcher.call(()->doClaim(owner,now,leaseMs));
    }
    @Override public boolean heartbeat(Claim c,long now,long leaseMs)throws IOException {
        if(leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        return dispatcher.call(()->{
            Map<String,Object> current=runs.get(c.execution());
            if(!Workflow.valid(current,c,now))return false;
            Map<String,Object> run=Workflow.mutableCopy(current);
            Workflow.heartbeat(run,c,now,leaseMs);commit("LeaseRenewed",run,now);return true;
        });
    }
    @Override public boolean complete(Claim c,Object result,String error,long now)throws IOException {
        Object owned=Json.deepCopy(result);
        return dispatcher.call(()->doComplete(c,owned,error,now));
    }
    /**
     * Completes a task and claims the next due one in a single group-commit command. Both
     * transitions share one flush, halving the durable round trips a busy worker waits for.
     */
    @Override public Handoff completeAndClaim(Claim c,Object result,String error,long now,String owner,long leaseMs)throws IOException {
        if(owner==null||owner.isBlank()||leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        Object owned=Json.deepCopy(result);
        return dispatcher.call(()->{
            boolean accepted=doComplete(c,owned,error,now);
            return new Handoff(accepted,doClaim(owner,now,leaseMs));
        });
    }
    private boolean doComplete(Claim c,Object result,String error,long now) {
        Map<String,Object> current=runs.get(c.execution());
        if(!Workflow.valid(current,c,now))return false;
        Map<String,Object> run=Workflow.mutableCopy(current);
        commit(Workflow.complete(run,c,result,error,now,sequence+1),run,now);return true;
    }
    @Override public void signal(String id,String step,long now)throws IOException {
        dispatcher.call(()->{
            Map<String,Object> run=writerCopy(id);
            Workflow.signal(run,step,now);commit("ApprovalReceived",run,now);return null;
        });
    }
    @Override public void cancel(String id,long now)throws IOException {
        dispatcher.call(()->{
            Map<String,Object> run=writerCopy(id);
            if(Workflow.cancel(run,now))commit("ExecutionCancelled",run,now);
            return null;
        });
    }
    @Override public Map<String,Object> get(String id) {
        Map<String,Object> run=committed.get(id);
        if(run==null)throw new NoSuchElementException("Execution not found");
        return Json.copy(run);
    }
    /** Returns once the execution is durably terminal, or its current state after {@code timeoutMs}. */
    @Override public Map<String,Object> awaitTerminal(String id,long timeoutMs)throws InterruptedException {
        CompletableFuture<Void> done=new CompletableFuture<>();
        // Register before checking, so a completion published in between cannot be missed.
        terminalWaiters.computeIfAbsent(id,k->new CopyOnWriteArrayList<>()).add(done);
        try {
            Map<String,Object> run=committed.get(id);
            if(run==null)throw new NoSuchElementException("Execution not found");
            if(!Workflow.terminal(Workflow.status(run)))
                try{done.get(timeoutMs,TimeUnit.MILLISECONDS);}catch(TimeoutException|ExecutionException ignored){}
            return get(id);
        } finally {
            terminalWaiters.computeIfPresent(id,(k,list)->{list.remove(done);return list.isEmpty()?null:list;});
        }
    }
    @Override public List<Object> list(int limit) {
        List<Object> result=new ArrayList<>(Math.min(limit,1000));
        synchronized(order) {
            for(int i=order.size()-1;i>=0&&result.size()<limit;i--)result.add(Workflow.summary(committed.get(order.get(i))));
        }
        return result;
    }
    @Override public Map<String,Object> stats() {
        Map<String,Long> statuses;int executions;
        synchronized(order){statuses=new TreeMap<>(statusCounts);executions=order.size();}
        long active=statuses.entrySet().stream().filter(e->!Workflow.terminal(e.getKey())).mapToLong(Map.Entry::getValue).sum();
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("backend",backend());m.put("executions",executions);m.put("active",active);
        m.put("durableTransitions",committedSequence);m.put("statuses",statuses);m.put("journal",journal.stats());m.put("writer",dispatcher.stats());
        return m;
    }
    @Override public boolean healthy(){return dispatcher.healthy();}
    @Override public String backend(){return "journal";}

    @Override public void awaitWork(long seenGeneration,long maxWaitMs)throws InterruptedException {
        signal.await(seenGeneration,Math.min(maxWaitMs,Math.max(0,nextDue-System.currentTimeMillis())));
    }
    @Override public long workGeneration(){return signal.generation();}
    /** Earliest due time of any queued task or lease expiry (may be stale-early, never late). */
    long nextDue(){return nextDue;}
    private void signalWork(){if(nextDue<=System.currentTimeMillis())signal.signal();}

    /**
     * Executor thread, after each command: encode the transitions it produced and capture the due
     * time they imply. Readers and claimers see neither until the flusher has made them durable.
     */
    private DurableDispatcher.Staged stageCommand()throws IOException {
        List<Map.Entry<CompletableFuture<Claim>,Claim>> grants=grantToIdle();
        List<Pending> staged=pending.isEmpty()?List.of():new ArrayList<>(pending);
        pending.clear();
        byte[] frames=new byte[0];
        if(!staged.isEmpty()) {
            List<Map<String,Object>> records=new ArrayList<>(staged.size());
            for(Pending p:staged)records.add(p.record());
            frames=journal.encode(records);
        }
        long dueSnapshot=due.isEmpty()?Long.MAX_VALUE:due.peek().at();
        return new DurableDispatcher.Staged(frames,staged.size(),()->{
            for(Pending p:staged)publish(p.run(),p.sequence());
            nextDue=dueSnapshot;
            for(var g:grants)g.getKey().complete(g.getValue());
            signalWork();
        });
    }
    /**
     * Executor thread: hands due tasks to parked workers inside the current batch, so the claim is
     * made durable by the same fsync as the transition that made the task due, instead of the worker
     * waking afterwards and spending a flush of its own. Grants are released only after that flush.
     */
    private List<Map.Entry<CompletableFuture<Claim>,Claim>> grantToIdle() {
        if(idle.isEmpty())return List.of();
        List<Map.Entry<CompletableFuture<Claim>,Claim>> grants=new ArrayList<>();
        long now=System.currentTimeMillis();
        while(!idle.isEmpty()&&!due.isEmpty()&&due.peek().at()<=now) {
            Idle waiter=idle.peek();
            if(waiter.grant().isDone()){idle.poll();continue;}
            Claim claim=doClaim(waiter.owner(),now,waiter.leaseMs());
            if(claim==null)break;
            idle.poll();grants.add(Map.entry(waiter.grant(),claim));
        }
        return grants;
    }
    @Override public Claim claimOrWait(String owner,long leaseMs,long maxWaitMs)throws IOException,InterruptedException {
        if(!DIRECT_GRANT)return LocalEngine.super.claimOrWait(owner,leaseMs,maxWaitMs);
        if(owner==null||owner.isBlank()||leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        Idle waiter=new Idle(owner,leaseMs,new CompletableFuture<>());
        Claim immediate=dispatcher.call(()->{
            Claim claim=doClaim(owner,System.currentTimeMillis(),leaseMs);
            if(claim==null&&maxWaitMs>0)idle.add(waiter);
            return claim;
        });
        if(immediate!=null||maxWaitMs<=0)return immediate;
        // Timers fall due without any command running; never sleep past the next due time.
        long wait=Math.max(1,Math.min(maxWaitMs,nextDue-System.currentTimeMillis()));
        try{return waiter.grant().get(wait,TimeUnit.MILLISECONDS);}
        catch(TimeoutException e) {
            // Withdraw. If the writer granted meanwhile, the grant is durable or about to be: use it.
            boolean withdrawn=dispatcher.call(()->idle.remove(waiter));
            if(withdrawn)return claim(owner,System.currentTimeMillis(),leaseMs);
            while(true) {
                try{return waiter.grant().get(1,TimeUnit.SECONDS);}
                catch(ExecutionException ex){throw new IOException(ex.getCause());}
                catch(TimeoutException stillFlushing) {
                    // A failed flush never releases its grants; do not wait for one forever.
                    if(!healthy())throw new IOException("Durability failure; restart required");
                }
            }
        }
        catch(ExecutionException e){throw new IOException(e.getCause());}
    }
    private Map<String,Object> writerCopy(String id) {
        Map<String,Object> run=runs.get(id);
        if(run==null)throw new NoSuchElementException("Execution not found");
        return Workflow.mutableCopy(run);
    }
    private String doStart(Map<String,Object> definition,String key,long now) {
        Workflow.validate(definition);Workflow.requireKey(key);
        String existing=requests.get(key);
        if(existing!=null) {
            if(!Json.write(runs.get(existing).get("definition")).equals(Json.write(definition)))
                throw new IllegalArgumentException("Idempotency key reused with a different definition");
            return existing;
        }
        String id=ids.get();
        commit("ExecutionStarted",Workflow.newRun(definition,key,id,now),now);
        return id;
    }
    private Claim doClaim(String owner,long now,long leaseMs) {
        // Entries are validated lazily: a task whose due time changed has a newer entry queued.
        while(!due.isEmpty()&&due.peek().at()<=now) {
            Due entry=due.poll();
            Map<String,Object> current=runs.get(entry.execution());
            if(current==null||Workflow.terminal(Workflow.status(current))){indexed.remove(key(entry.execution(),entry.task()));continue;}
            Object task=Workflow.tasks(current).get(entry.task());
            if(task==null||Workflow.dueAt(Json.obj(task))!=entry.at())continue;
            Map<String,Object> run=Workflow.mutableCopy(current);
            Workflow.ClaimResult result=Workflow.claim(run,entry.task(),owner,now,leaseMs);
            commit(result.event(),run,now);
            if(result.claim()!=null)return result.claim();
        }
        return null;
    }
    private void commit(String type,Map<String,Object> run,long now) {
        run.put("updatedAt",now);
        String id=Json.str(run,"id","");
        Map<String,Object> previous=runs.get(id);
        Map<String,Object> patch=previous==null||FULL_RECORDS?null:Patch.diff(previous,run);
        Map<String,Object> record=new LinkedHashMap<>();
        record.put("sequence",sequence+1);record.put("type",type);record.put("at",now);
        if(patch==null)record.put("execution",run);
        else{record.put("id",id);record.put("patch",patch);}
        // Only the writer thread sees this speculative state. Replies and publication wait for flush.
        pending.add(new Pending(record,run,sequence+1));install(run,sequence+1);
    }
    private void replay(Map<String,Object> record) {
        Map<String,Object> run;
        if(record.containsKey("execution"))run=Json.obj(record.get("execution"));
        else {
            String id=Json.str(record,"id","");
            Map<String,Object> previous=runs.get(id);
            if(previous==null)throw new IllegalStateException("Patch for unknown execution "+id);
            run=Patch.apply(previous,Json.obj(record.get("patch")));
        }
        long seq=Json.num(record,"sequence",0);
        install(run,seq);publish(run,seq);
    }
    private void install(Map<String,Object> run,long next) {
        if(next!=sequence+1)throw new IllegalStateException("Non-contiguous event sequence");
        sequence=next;
        String id=Json.str(run,"id","");
        runs.put(id,run);requests.put(Json.str(run,"requestKey",""),id);
        boolean terminal=Workflow.terminal(Workflow.status(run));
        for(var e:Workflow.tasks(run).entrySet()) {
            String k=key(id,e.getKey());
            long at=terminal?Long.MAX_VALUE:Workflow.dueAt(Json.obj(e.getValue()));
            if(at==Long.MAX_VALUE){indexed.remove(k);continue;}
            Long previous=indexed.put(k,at);
            if(previous==null||previous!=at)due.add(new Due(at,dueOrder++,id,e.getKey()));
        }
    }
    private void publish(Map<String,Object> run,long seq) {
        String id=Json.str(run,"id","");
        synchronized(order) {
            Map<String,Object> previous=committed.put(id,run);
            if(previous==null)order.add(id);
            else statusCounts.merge(Workflow.status(previous),-1L,(a,b)->a+b==0?null:a+b);
            statusCounts.merge(Workflow.status(run),1L,Long::sum);
        }
        committedSequence=seq;
        if(Workflow.terminal(Workflow.status(run))) {
            List<CompletableFuture<Void>> waiting=terminalWaiters.get(id);
            if(waiting!=null)waiting.forEach(f->f.complete(null));
        }
    }
    private static String key(String execution,String task){return execution+'\u0000'+task;}
    @Override public void close()throws IOException {dispatcher.close();journal.close();}
}
