package dev.conductor.aws;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Dashboard counters over sharded {@code STATS#n} items. Exact counters (executions, per-status
 * counts, effects) are incremented inside the transaction that causes them, on a random shard to
 * spread contention. Throughput counters (transitions, renewals, retries) are accumulated locally
 * and flushed once per second; a crash can lose up to one second of those. Nothing that decides
 * workflow behavior reads any counter.
 */
final class Counters implements AutoCloseable {
    static final int SHARDS=32;
    private final ConcurrentHashMap<String,LongAdder> deltas=new ConcurrentHashMap<>();
    private final ScheduledExecutorService flusher=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("counter-flush").factory());
    private final DynamoDbClient dynamo;
    private final String table;
    private final String shard="STATS#"+ThreadLocalRandom.current().nextInt(SHARDS);
    Counters(DynamoDbClient dynamo,String table) {
        this.dynamo=dynamo;this.table=table;
        flusher.scheduleWithFixedDelay(this::flushQuietly,1,1,TimeUnit.SECONDS);
    }
    void add(String name,long delta){if(delta!=0)deltas.computeIfAbsent(name,k->new LongAdder()).add(delta);}
    /** A transactional increment for exact counters; {@code deltas} pairs names with amounts. */
    TransactWriteItem increment(Map<String,Long> deltas) {
        StringBuilder expression=new StringBuilder("ADD ");
        Map<String,String> names=new HashMap<>();Map<String,AttributeValue> values=new HashMap<>();
        int i=0;
        for(var e:deltas.entrySet()) {
            if(i>0)expression.append(", ");
            expression.append("#x").append(i).append(" :x").append(i);
            names.put("#x"+i,e.getKey());values.put(":x"+i,AttributeValue.fromN(Long.toString(e.getValue())));i++;
        }
        return TransactWriteItem.builder().update(Update.builder().tableName(table)
                .key(Map.of("pk",AttributeValue.fromS("STATS#"+ThreadLocalRandom.current().nextInt(SHARDS)),"sk",AttributeValue.fromS("COUNTERS")))
                .updateExpression(expression.toString()).expressionAttributeNames(names).expressionAttributeValues(values).build()).build();
    }
    private void flushQuietly(){try{flush();}catch(RuntimeException e){System.err.println("Counter flush deferred: "+e.getMessage());}}
    synchronized void flush() {
        Map<String,Long> batch=new LinkedHashMap<>();
        deltas.forEach((k,v)->{long n=v.sumThenReset();if(n!=0)batch.put(k,n);});
        if(batch.isEmpty())return;
        StringBuilder expression=new StringBuilder("ADD ");
        Map<String,String> names=new HashMap<>();Map<String,AttributeValue> values=new HashMap<>();
        int i=0;
        for(var e:batch.entrySet()) {
            if(i>0)expression.append(", ");
            expression.append("#c").append(i).append(" :c").append(i);
            names.put("#c"+i,e.getKey());values.put(":c"+i,AttributeValue.fromN(Long.toString(e.getValue())));i++;
        }
        try {
            dynamo.updateItem(UpdateItemRequest.builder().tableName(table)
                    .key(Map.of("pk",AttributeValue.fromS(shard),"sk",AttributeValue.fromS("COUNTERS")))
                    .updateExpression(expression.toString()).expressionAttributeNames(names).expressionAttributeValues(values).build());
        } catch(RuntimeException e) {
            batch.forEach(this::add);throw e;
        }
    }
    Map<String,Long> read() {
        Map<String,Long> totals=new TreeMap<>();
        List<Map<String,AttributeValue>> keys=new ArrayList<>();
        for(int i=0;i<SHARDS;i++)keys.add(Map.of("pk",AttributeValue.fromS("STATS#"+i),"sk",AttributeValue.fromS("COUNTERS")));
        Map<String,KeysAndAttributes> request=Map.of(table,KeysAndAttributes.builder().keys(keys).consistentRead(true).build());
        while(!request.isEmpty()) {
            BatchGetItemResponse response=dynamo.batchGetItem(BatchGetItemRequest.builder().requestItems(request).build());
            for(Map<String,AttributeValue> item:response.responses().getOrDefault(table,List.of()))
                item.forEach((k,v)->{if(v.n()!=null)totals.merge(k,Long.parseLong(v.n()),Long::sum);});
            request=response.unprocessedKeys();
        }
        // Include this process's unflushed deltas so a single-process view is read-your-writes.
        deltas.forEach((k,v)->totals.merge(k,v.sum(),Long::sum));
        return totals;
    }
    @Override public void close() {
        flusher.shutdown();
        try{flusher.awaitTermination(2,TimeUnit.SECONDS);flush();}
        catch(InterruptedException e){Thread.currentThread().interrupt();}
        catch(RuntimeException e){System.err.println("Final counter flush failed: "+e.getMessage());}
    }
}
