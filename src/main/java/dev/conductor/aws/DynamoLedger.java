package dev.conductor.aws;

import dev.conductor.core.*;
import java.util.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Example downstream effect store: one conditional put stores the idempotency key and the effect
 * together, so a retried attempt observes the first attempt's result instead of applying twice.
 */
public final class DynamoLedger implements EffectLedger {
    private final DynamoDbClient dynamo;
    private final String table;
    private final Counters counters;
    public DynamoLedger(DynamoEngine engine,AwsClients aws) {
        this.dynamo=aws.dynamo;this.table=engine.table();this.counters=engine.counters();
    }
    @Override public Object apply(String key,Object value) {
        String json=Json.write(Json.deepCopy(value));
        Map<String,AttributeValue> id=Map.of("pk",DynamoEngine.s("EFFECT#"+key),"sk",DynamoEngine.s("EFFECT"));
        Map<String,AttributeValue> item=new HashMap<>(id);
        item.put("value",DynamoEngine.s(json));item.put("createdAt",DynamoEngine.n(System.currentTimeMillis()));
        try {
            dynamo.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
                    TransactWriteItem.builder().put(Put.builder().tableName(table).item(item).conditionExpression("attribute_not_exists(pk)").build()).build(),
                    counters.increment(Map.of("effects",1L))).build());
            return Json.parse(json);
        } catch(TransactionCanceledException e) {
            var reasons=e.cancellationReasons();
            if(reasons==null||reasons.isEmpty()||!"ConditionalCheckFailed".equals(reasons.get(0).code()))throw e;
            String existing=dynamo.getItem(GetItemRequest.builder().tableName(table).key(id).consistentRead(true).build()).item().get("value").s();
            if(!existing.equals(json))throw new IllegalArgumentException("Idempotency payload mismatch");
            return Json.parse(existing);
        }
    }
    @Override public long size(){return counters.read().getOrDefault("effects",0L);}
    @Override public void close() {}
}
