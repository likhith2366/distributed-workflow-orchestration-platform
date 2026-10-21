package dev.conductor.aws;

import dev.conductor.core.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.eventbridge.model.*;
import software.amazon.awssdk.services.sqs.model.*;

/**
 * Horizontally scalable backend. Each execution is an independent optimistic-concurrency unit:
 *
 * <ol>
 *   <li>read {@code EXEC#id/META} (strongly consistent) and apply the pure {@link Workflow} transition;</li>
 *   <li>in one DynamoDB transaction, conditionally replace META at {@code version+1}, append
 *       {@code EVT#version}, and write an outbox item naming the SQS messages and EventBridge
 *       events the transition requires;</li>
 *   <li>after commit, send those messages and delete the outbox item. A relay re-sends any outbox
 *       item a crashed process left behind, so dispatch is at-least-once and never lost.</li>
 * </ol>
 *
 * Workers claim from SQS messages with the same conditional write, so duplicate or stale messages
 * are harmless: they fail the state check and are dropped. Lease validity uses the clock of the
 * process performing the write; fencing tokens keep stale owners out regardless of clock skew.
 */
public final class DynamoEngine implements WorkflowEngine {
    static final int LIST_SHARDS=4,OUTBOX_SHARDS=8;
    /** Maximum SQS DelaySeconds. Later timers wait in the DynamoDB outbox, not in SQS. */
    static final long NEAR_MS=900_000;
    private static final int MAX_ITEM_BYTES=380_000;

    /** An SQS dispatch message. {@code kind} is "ready" (claim a due task) or "lease" (check lease expiry). */
    public record Message(String execution,String task,String kind,long fence,long dueAt) {
        public String toJson(){return Json.write(Map.of("e",execution,"t",task,"k",kind,"f",fence,"d",dueAt));}
        public static Message parse(String body) {
            Map<String,Object> m=Json.obj(Json.parse(body));
            String kind=Json.str(m,"k","");
            if(!kind.equals("ready")&&!kind.equals("lease"))throw new IllegalArgumentException("Unknown message kind");
            return new Message(Json.str(m,"e",""),Json.str(m,"t",""),kind,Json.num(m,"f",0),Json.num(m,"d",0));
        }
    }
    public sealed interface TaskOutcome permits Claimed,NotDue,Gone {}
    public record Claimed(Claim claim) implements TaskOutcome {}
    public record NotDue(long dueAt) implements TaskOutcome {}
    public record Gone() implements TaskOutcome {}

    private record Change<T>(String event,T value,boolean journaled) {
        static <T> Change<T> none(T value){return new Change<>(null,value,false);}
    }
    @FunctionalInterface private interface Mutator<T> {Change<T> apply(Map<String,Object> run,long nextVersion);}
    private record Loaded(long version,Map<String,Object> run,String body) {}
    private record OutboxItem(String pk,String sk,long dueAt,List<Message> messages,List<Map<String,Object>> events) {}

    private final AwsClients aws;
    private final String table,queueUrl,eventBus;
    private final PayloadStore payloads;
    private final Counters counters;

    public DynamoEngine(AwsClients aws,AwsSettings settings,String queueUrl) {
        this.aws=aws;this.table=settings.table();this.queueUrl=queueUrl;this.eventBus=settings.eventBus();
        payloads=new PayloadStore(aws.s3,settings.bucket(),settings.payloadThresholdBytes());
        counters=new Counters(aws.dynamo,table);
    }
    Counters counters(){return counters;}
    String table(){return table;}

    // ---- WorkflowEngine -------------------------------------------------------------------

    @Override public String start(Map<String,Object> definition,String key,long now)throws IOException {
        Map<String,Object> owned=Json.copy(definition);
        Workflow.validate(owned);Workflow.requireKey(key);
        String hash=PayloadStore.sha256(Json.write(owned).getBytes(StandardCharsets.UTF_8));
        String existing=existingSubmission(key,hash);
        if(existing!=null)return existing;
        String id=UUID.randomUUID().toString();
        Map<String,Object> run=Workflow.newRun(owned,key,id,now);
        List<TransactWriteItem> writes=new ArrayList<>();
        writes.add(TransactWriteItem.builder().put(Put.builder().tableName(table)
                .item(Map.of("pk",s("SUBMIT#"+key),"sk",s("RESULT"),"executionId",s(id),"definitionHash",s(hash),"createdAt",n(now)))
                .conditionExpression("attribute_not_exists(pk)").build()).build());
        List<OutboxItem> outbox=prepare(null,run,"ExecutionStarted",1,now,writes,"attribute_not_exists(pk)",null);
        try {
            aws.dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes).build());
        } catch(TransactionCanceledException e) {
            // A concurrent request with the same key won the race.
            if(reason(e,0).equals("ConditionalCheckFailed")) {
                String winner=existingSubmission(key,hash);
                if(winner!=null)return winner;
            }
            throw new IOException("Start transaction cancelled: "+reasons(e),e);
        }
        counters.add("transitions",1);
        dispatchNow(outbox,now);
        return id;
    }
    private String existingSubmission(String key,String hash) {
        Map<String,AttributeValue> item=aws.dynamo.getItem(GetItemRequest.builder().tableName(table)
                .key(key("SUBMIT#"+key,"RESULT")).consistentRead(true).build()).item();
        if(item==null||item.isEmpty())return null;
        if(!item.get("definitionHash").s().equals(hash))throw new IllegalArgumentException("Idempotency key reused with a different definition");
        return item.get("executionId").s();
    }
    @Override public Claim claim(String owner,long now,long leaseMs) {
        throw new java.lang.UnsupportedOperationException("The dynamodb backend dispatches tasks through SQS; run SQS workers");
    }
    /** Attempts the claim named by an SQS message. */
    public TaskOutcome claimTask(String owner,Message m,long now,long leaseMs)throws IOException {
        try {
            return mutate(m.execution(),now,(run,next)->{
                if(Workflow.terminal(Workflow.status(run)))return Change.none(new Gone());
                Object value=Workflow.tasks(run).get(m.task());
                if(value==null)return Change.none(new Gone());
                Map<String,Object> task=Json.obj(value);String status=Workflow.status(task);
                if(m.kind().equals("ready")) {
                    if(!status.equals("READY"))return Change.none(new Gone());
                } else if(!status.equals("RUNNING")||Json.num(task,"fence",0)!=m.fence())return Change.none(new Gone());
                long due=Workflow.dueAt(task);
                if(due>now)return Change.none(new NotDue(due));
                Workflow.ClaimResult r=Workflow.claim(run,m.task(),owner,now,leaseMs);
                return new Change<>(r.event(),r.claim()==null?new Gone():new Claimed(r.claim()),true);
            });
        } catch(NoSuchElementException e){return new Gone();}
    }
    /** Re-enqueues a message that arrived before its task was due. */
    public void requeue(Message m,long dueAt,long now) {
        aws.sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl)
                .messageBody(new Message(m.execution(),m.task(),m.kind(),m.fence(),dueAt).toJson())
                .delaySeconds(delaySeconds(dueAt,now)).build());
    }
    @Override public boolean heartbeat(Claim c,long now,long leaseMs)throws IOException {
        if(leaseMs<1)throw new IllegalArgumentException("Invalid lease");
        return mutate(c.execution(),now,(run,next)->{
            if(!Workflow.valid(run,c,now))return Change.none(false);
            Workflow.heartbeat(run,c,now,leaseMs);
            return new Change<>("LeaseRenewed",true,false);
        });
    }
    @Override public boolean complete(Claim c,Object result,String error,long now)throws IOException {
        Object owned=Json.deepCopy(result);
        return mutate(c.execution(),now,(run,next)->{
            if(!Workflow.valid(run,c,now))return Change.none(false);
            return new Change<>(Workflow.complete(run,c,Json.deepCopy(owned),error,now,next),true,true);
        });
    }
    @Override public void signal(String id,String step,long now)throws IOException {
        mutate(id,now,(run,next)->{Workflow.signal(run,step,now);return new Change<>("ApprovalReceived",null,true);});
    }
    @Override public void cancel(String id,long now)throws IOException {
        mutate(id,now,(run,next)->Workflow.cancel(run,now)?new Change<>("ExecutionCancelled",null,true):Change.none(null));
    }
    @Override public Map<String,Object> get(String id) {
        Map<String,Object> run=load(id).run();
        payloads.resolve(run);
        return run;
    }
    @Override public List<Object> list(int limit) {
        List<Map<String,AttributeValue>> items=new ArrayList<>();
        for(int shard=0;shard<LIST_SHARDS;shard++) {
            items.addAll(aws.dynamo.query(QueryRequest.builder().tableName(table).indexName(AwsResources.LIST_INDEX)
                    .keyConditionExpression("gsi1pk = :p").expressionAttributeValues(Map.of(":p",s("RUNS#"+shard)))
                    .scanIndexForward(false).limit(limit).build()).items());
        }
        items.sort(Comparator.comparing((Map<String,AttributeValue> i)->i.get("gsi1sk").s()).reversed());
        List<Object> result=new ArrayList<>();
        for(Map<String,AttributeValue> item:items.subList(0,Math.min(limit,items.size()))) {
            Map<String,Object> summary=new LinkedHashMap<>();
            summary.put("id",item.get("id").s());summary.put("status",item.get("status").s());
            summary.put("createdAt",Long.parseLong(item.get("createdAt").n()));
            summary.put("updatedAt",Long.parseLong(item.get("updatedAt").n()));summary.put("name",item.get("name").s());
            result.add(summary);
        }
        return result;
    }
    @Override public Map<String,Object> stats() {
        Map<String,Long> totals=counters.read();
        Map<String,Long> statuses=new TreeMap<>();
        totals.forEach((k,v)->{if(k.startsWith("status:")&&v!=0)statuses.put(k.substring(7),v);});
        long active=statuses.entrySet().stream().filter(e->!Workflow.terminal(e.getKey())).mapToLong(Map.Entry::getValue).sum();
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("backend",backend());m.put("executions",totals.getOrDefault("executions",0L));m.put("active",active);
        m.put("durableTransitions",totals.getOrDefault("transitions",0L));m.put("statuses",statuses);
        m.put("leaseRenewals",totals.getOrDefault("leaseRenewals",0L));m.put("contentionRetries",totals.getOrDefault("contentionRetries",0L));
        m.put("outboxRelayed",totals.getOrDefault("outboxRelayed",0L));m.put("bestEffortCounters",List.of("durableTransitions","leaseRenewals","contentionRetries","outboxRelayed"));
        return m;
    }
    long effects(){return counters.read().getOrDefault("effects",0L);}
    @Override public boolean healthy(){return true;}
    @Override public String backend(){return "dynamodb";}
    @Override public void close(){counters.close();}

    // ---- Transitions ----------------------------------------------------------------------

    private Loaded load(String id) {
        Map<String,AttributeValue> item=aws.dynamo.getItem(GetItemRequest.builder().tableName(table)
                .key(key("EXEC#"+id,"META")).consistentRead(true).build()).item();
        if(item==null||item.isEmpty())throw new NoSuchElementException("Execution not found");
        String body=item.get("body").s();
        return new Loaded(Long.parseLong(item.get("version").n()),Json.obj(Json.parse(body)),body);
    }
    private <T> T mutate(String id,long now,Mutator<T> fn)throws IOException {
        for(int attempt=0;;attempt++) {
            Loaded loaded=load(id);
            Map<String,Object> before=Json.obj(Json.parse(loaded.body()));
            long next=loaded.version()+1;
            Change<T> change=fn.apply(loaded.run(),next);
            if(change.event()==null)return change.value();
            String condition="version = :expected";
            Map<String,AttributeValue> expected=Map.of(":expected",n(loaded.version()));
            try {
                if(!change.journaled()) {
                    // Lease renewals replace META only: no history entry and no dispatch.
                    payloads.offload(loaded.run());
                    aws.dynamo.putItem(PutItemRequest.builder().tableName(table).item(meta(loaded.run(),next))
                            .conditionExpression(condition).expressionAttributeValues(expected).build());
                    counters.add("leaseRenewals",1);
                    return change.value();
                }
                List<TransactWriteItem> writes=new ArrayList<>();
                List<OutboxItem> outbox=prepare(before,loaded.run(),change.event(),next,now,writes,condition,expected);
                aws.dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes).build());
                counters.add("transitions",1);
                dispatchNow(outbox,now);
                return change.value();
            } catch(ConditionalCheckFailedException e) {
                retryPause(id,attempt,e);
            } catch(TransactionCanceledException e) {
                String reason=reasons(e);
                if(!reason.contains("ConditionalCheckFailed")&&!reason.contains("TransactionConflict"))
                    throw new IOException("Transition cancelled: "+reason,e);
                retryPause(id,attempt,e);
            }
        }
    }
    private void retryPause(String id,int attempt,Exception cause)throws IOException {
        counters.add("contentionRetries",1);
        if(attempt>=24)throw new IOException("Execution "+id+" is too contended; retry the request",cause);
        try{Thread.sleep(ThreadLocalRandom.current().nextLong(1,2L<<Math.min(attempt,7)));}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new IOException("Interrupted during retry",e);}
    }

    /** Appends META, EVT and outbox writes for a journaled transition. */
    private List<OutboxItem> prepare(Map<String,Object> before,Map<String,Object> after,String event,long version,long now,
                                     List<TransactWriteItem> writes,String condition,Map<String,AttributeValue> expected) {
        payloads.offload(after);
        String id=Json.str(after,"id","");
        Put.Builder metaPut=Put.builder().tableName(table).item(meta(after,version)).conditionExpression(condition);
        if(expected!=null)metaPut.expressionAttributeValues(expected);
        writes.add(TransactWriteItem.builder().put(metaPut.build()).build());

        Map<String,Object> changes=new LinkedHashMap<>();
        List<Message> near=new ArrayList<>();Map<Long,List<Message>> far=new TreeMap<>();
        Map<String,Object> beforeTasks=before==null?Map.of():Workflow.tasks(before);
        for(var e:Workflow.tasks(after).entrySet()) {
            Map<String,Object> a=Json.obj(e.getValue());
            Map<String,Object> b=beforeTasks.containsKey(e.getKey())?Json.obj(beforeTasks.get(e.getKey())):null;
            Map<String,Object> visible=visible(a);
            if(b==null||!visible.equals(visible(b)))changes.put(e.getKey(),visible);
            Message message=null;
            String status=Workflow.status(a);
            if(status.equals("READY")&&(b==null||!Workflow.status(b).equals("READY")||Json.num(a,"readyAt",0)!=Json.num(b,"readyAt",0)
                    ||Json.num(a,"attempt",0)!=Json.num(b,"attempt",0)))
                message=new Message(id,e.getKey(),"ready",Json.num(a,"fence",0),Json.num(a,"readyAt",now));
            else if(status.equals("RUNNING")&&(b==null||Json.num(a,"fence",0)!=Json.num(b,"fence",0)))
                message=new Message(id,e.getKey(),"lease",Json.num(a,"fence",0),Json.num(a,"leaseUntil",now));
            if(message==null)continue;
            if(message.dueAt()<=now+NEAR_MS)near.add(message);else far.computeIfAbsent(message.dueAt(),k->new ArrayList<>()).add(message);
        }
        Map<String,AttributeValue> evt=new HashMap<>();
        evt.put("pk",s("EXEC#"+id));evt.put("sk",s(String.format("EVT#%020d",version)));
        evt.put("type",s(event));evt.put("at",n(now));evt.put("status",s(Workflow.status(after)));
        evt.put("changes",s(Json.write(changes)));
        writes.add(TransactWriteItem.builder().put(Put.builder().tableName(table).item(evt)
                .conditionExpression("attribute_not_exists(pk)").build()).build());

        List<Map<String,Object>> events=new ArrayList<>();
        String previous=before==null?null:Workflow.status(before);
        if(!Objects.equals(previous,Workflow.status(after))) {
            Map<String,Long> deltas=new LinkedHashMap<>();
            if(previous==null)deltas.put("executions",1L);else deltas.put("status:"+previous,-1L);
            deltas.put("status:"+Workflow.status(after),1L);
            writes.add(counters.increment(deltas));
            Map<String,Object> detail=new LinkedHashMap<>();
            Map<String,Object> definition=Json.obj(after.get("definition"));
            detail.put("executionId",id);detail.put("workflow",definition.get("name"));detail.put("version",definition.get("version"));
            detail.put("status",Workflow.status(after));detail.put("previousStatus",previous);detail.put("transition",event);detail.put("at",now);
            events.add(detail);
        }
        List<OutboxItem> outbox=new ArrayList<>();
        String pk="OUTBOX#"+Math.floorMod(id.hashCode(),OUTBOX_SHARDS);
        if(!near.isEmpty()||!events.isEmpty()) {
            long due=near.stream().mapToLong(Message::dueAt).min().orElse(now);
            outbox.add(new OutboxItem(pk,String.format("%015d#%s#%020d#0",Math.min(due,now),id,version),now,near,events));
        }
        int index=1;
        for(var e:far.entrySet())
            outbox.add(new OutboxItem(pk,String.format("%015d#%s#%020d#%d",e.getKey(),id,version,index++),e.getKey(),e.getValue(),List.of()));
        if(writes.size()+outbox.size()>100)throw new IllegalArgumentException("Transition schedules too many distinct timers");
        for(OutboxItem o:outbox) {
            writes.add(TransactWriteItem.builder().put(Put.builder().tableName(table).item(Map.of(
                    "pk",s(o.pk()),"sk",s(o.sk()),"dueAt",n(o.dueAt()),"createdAt",n(now),
                    "payload",s(Json.write(Map.of("messages",o.messages().stream().map(m->Json.parse(m.toJson())).toList(),"events",o.events())))
            )).build()).build());
        }
        return outbox;
    }
    private static Map<String,Object> visible(Map<String,Object> task) {
        Map<String,Object> v=new LinkedHashMap<>();
        for(String k:List.of("status","attempt","fence","owner","readyAt","leaseUntil","error","completedAt"))
            if(task.containsKey(k))v.put(k,task.get(k));
        return v;
    }
    private Map<String,AttributeValue> meta(Map<String,Object> run,long version) {
        String id=Json.str(run,"id",""),body=Json.write(run);
        if(body.length()>MAX_ITEM_BYTES)throw new IllegalArgumentException("Execution state exceeds the DynamoDB item budget; reduce definition size");
        long createdAt=Json.num(run,"createdAt",0);
        Map<String,AttributeValue> item=new HashMap<>();
        item.put("pk",s("EXEC#"+id));item.put("sk",s("META"));item.put("version",n(version));
        item.put("id",s(id));item.put("status",s(Workflow.status(run)));
        item.put("name",s(Json.str(Json.obj(run.get("definition")),"name","")));
        item.put("createdAt",n(createdAt));item.put("updatedAt",n(Json.num(run,"updatedAt",createdAt)));
        item.put("body",s(body));
        item.put("gsi1pk",s("RUNS#"+Math.floorMod(id.hashCode(),LIST_SHARDS)));
        item.put("gsi1sk",s(String.format("%015d#%s",createdAt,id)));
        return item;
    }

    // ---- Outbox dispatch ------------------------------------------------------------------

    private void dispatchNow(List<OutboxItem> outbox,long now) {
        for(OutboxItem item:outbox) {
            if(item.dueAt()>now+NEAR_MS)continue;
            try{dispatch(item,now);}
            catch(RuntimeException e){System.err.println("Outbox dispatch deferred to relay: "+e.getMessage());}
        }
    }
    private void dispatch(OutboxItem item,long now) {
        List<Message> messages=item.messages();
        for(int from=0;from<messages.size();from+=10) {
            List<SendMessageBatchRequestEntry> entries=new ArrayList<>();
            for(int i=from;i<Math.min(messages.size(),from+10);i++) {
                Message m=messages.get(i);
                entries.add(SendMessageBatchRequestEntry.builder().id(Integer.toString(i)).messageBody(m.toJson())
                        .delaySeconds(delaySeconds(m.dueAt(),now)).build());
            }
            SendMessageBatchResponse response=aws.sqs.sendMessageBatch(SendMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build());
            if(!response.failed().isEmpty())throw new IllegalStateException("SQS rejected "+response.failed().size()+" messages");
        }
        List<Map<String,Object>> events=item.events();
        for(int from=0;from<events.size();from+=10) {
            List<PutEventsRequestEntry> entries=new ArrayList<>();
            for(Map<String,Object> detail:events.subList(from,Math.min(events.size(),from+10)))
                entries.add(PutEventsRequestEntry.builder().eventBusName(eventBus).source("dev.conductor")
                        .detailType("Execution Status Change").time(Instant.ofEpochMilli(Json.num(detail,"at",now)))
                        .detail(Json.write(detail)).build());
            PutEventsResponse response=aws.events.putEvents(PutEventsRequest.builder().entries(entries).build());
            if(response.failedEntryCount()!=null&&response.failedEntryCount()>0)throw new IllegalStateException("EventBridge rejected "+response.failedEntryCount()+" events");
        }
        aws.dynamo.deleteItem(DeleteItemRequest.builder().tableName(table).key(key(item.pk(),item.sk())).build());
    }
    /**
     * Sends outbox items that are within SQS delay range and older than {@code graceMs}, which
     * leaves fresh items to the committing process's fast path. Returns the number relayed.
     */
    public int relayOnce(long now,long graceMs) {
        int relayed=0;
        for(int shard=0;shard<OUTBOX_SHARDS;shard++) {
            List<Map<String,AttributeValue>> items=aws.dynamo.query(QueryRequest.builder().tableName(table)
                    .keyConditionExpression("pk = :p AND sk <= :hi").filterExpression("createdAt <= :created")
                    .expressionAttributeValues(Map.of(":p",s("OUTBOX#"+shard),":hi",s(String.format("%015d~",now+NEAR_MS)),
                            ":created",n(now-graceMs)))
                    .limit(50).build()).items();
            for(Map<String,AttributeValue> raw:items) {
                Map<String,Object> payload=Json.obj(Json.parse(raw.get("payload").s()));
                List<Message> messages=new ArrayList<>();
                for(Object m:Json.arr(payload.get("messages")))messages.add(Message.parse(Json.write(m)));
                List<Map<String,Object>> events=new ArrayList<>();
                for(Object e:Json.arr(payload.get("events")))events.add(Json.obj(e));
                dispatch(new OutboxItem(raw.get("pk").s(),raw.get("sk").s(),Long.parseLong(raw.get("dueAt").n()),messages,events),now);
                relayed++;
            }
        }
        counters.add("outboxRelayed",relayed);
        return relayed;
    }
    static int delaySeconds(long dueAt,long now) {
        return (int)Math.max(0,Math.min(NEAR_MS/1000,(dueAt-now+999)/1000));
    }

    private static Map<String,AttributeValue> key(String pk,String sk){return Map.of("pk",s(pk),"sk",s(sk));}
    static AttributeValue s(String v){return AttributeValue.fromS(v);}
    static AttributeValue n(long v){return AttributeValue.fromN(Long.toString(v));}
    private static String reason(TransactionCanceledException e,int index) {
        var reasons=e.cancellationReasons();
        return reasons==null||reasons.size()<=index||reasons.get(index).code()==null?"":reasons.get(index).code();
    }
    private static String reasons(TransactionCanceledException e) {
        return e.cancellationReasons()==null?String.valueOf(e.getMessage()):e.cancellationReasons().stream().map(r->String.valueOf(r.code())).toList().toString();
    }
}
