package dev.conductor.aws;

import static org.junit.jupiter.api.Assertions.*;

import dev.conductor.core.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.services.sqs.model.*;

/**
 * Runs against LocalStack (or a disposable AWS account):
 * {@code CONDUCTOR_IT_AWS_ENDPOINT=http://127.0.0.1:4566 mvn test -Dtest=DynamoEngineIT}
 */
@EnabledIfEnvironmentVariable(named="CONDUCTOR_IT_AWS_ENDPOINT",matches=".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DynamoEngineIT {
    AwsSettings settings;
    AwsClients aws;
    String queueUrl;
    DynamoEngine engine;

    @BeforeAll void setUp() {
        System.setProperty("aws.accessKeyId",System.getenv().getOrDefault("AWS_ACCESS_KEY_ID","test"));
        System.setProperty("aws.secretAccessKey",System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY","test"));
        String suffix=UUID.randomUUID().toString().substring(0,8);
        settings=new AwsSettings("us-east-1",System.getenv("CONDUCTOR_IT_AWS_ENDPOINT"),"jt-"+suffix,"jt-q-"+suffix,
                "jt-b-"+suffix,"jt-bus-"+suffix,true,2048);
        aws=new AwsClients(settings);
        queueUrl=AwsResources.ensure(aws,settings);
        engine=new DynamoEngine(aws,settings,queueUrl);
    }
    @AfterAll void tearDown(){engine.close();aws.close();}

    static Map<String,Object> def(String steps) {
        return Json.obj(Json.parse("{\"name\":\"it\",\"version\":\"1\",\"steps\":"+steps+"}"));
    }
    List<DynamoEngine.Message> drain(String execution,int expected)throws InterruptedException {
        List<DynamoEngine.Message> found=new ArrayList<>();
        long deadline=System.currentTimeMillis()+15_000;
        while(found.size()<expected&&System.currentTimeMillis()<deadline) {
            for(var m:aws.sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(1).build()).messages()) {
                DynamoEngine.Message parsed=DynamoEngine.Message.parse(m.body());
                aws.sqs.deleteMessage(b->b.queueUrl(queueUrl).receiptHandle(m.receiptHandle()));
                if(parsed.execution().equals(execution))found.add(parsed);
            }
        }
        return found;
    }

    @Test void claimCompleteAndFencingThroughMessages()throws Exception {
        long now=System.currentTimeMillis();
        String id=engine.start(def("[{\"id\":\"a\"},{\"id\":\"b\",\"dependsOn\":[\"a\"]}]"),"fence-"+UUID.randomUUID(),now);
        DynamoEngine.Message ready=drain(id,1).get(0);
        assertEquals("ready",ready.kind());
        var first=(DynamoEngine.Claimed)engine.claimTask("w1",ready,now,1000);
        assertInstanceOf(DynamoEngine.Gone.class,engine.claimTask("w2",ready,now,1000),"duplicate delivery is harmless");
        DynamoEngine.Message lease=drain(id,1).get(0);
        assertEquals("lease",lease.kind());
        assertInstanceOf(DynamoEngine.NotDue.class,engine.claimTask("w2",lease,now+500,1000),"lease still live");
        var second=(DynamoEngine.Claimed)engine.claimTask("w2",lease,now+1000,1000);
        assertEquals(2,second.claim().fence());
        assertFalse(engine.complete(first.claim(),1,null,now+1100),"stale owner fenced");
        assertTrue(engine.complete(second.claim(),1,null,now+1100));
        assertEquals("READY",Json.obj(Json.obj(engine.get(id).get("tasks")).get("b")).get("status"));
    }
    @Test void outboxRelayDeliversDispatchesAfterAFailedFastPath()throws Exception {
        // A process whose SQS sends fail models a crash between the commit and the dispatch.
        DynamoEngine crashed=new DynamoEngine(aws,settings,queueUrl+"-missing");
        long now=System.currentTimeMillis();
        String id=crashed.start(def("[{\"id\":\"a\"}]"),"relay-"+UUID.randomUUID(),now);
        crashed.close();
        assertTrue(drain(id,1).isEmpty(),"fast path did not deliver");
        int relayed=0;
        for(int i=0;i<10&&relayed==0;i++)relayed+=engine.relayOnce(System.currentTimeMillis()+5_000,2_000);
        assertTrue(relayed>=1);
        assertEquals("a",drain(id,1).get(0).task());
    }
    @Test void concurrentStartsWithOneKeyCreateOneExecution()throws Exception {
        String key="race-"+UUID.randomUUID();
        Set<String> ids=ConcurrentHashMap.newKeySet();
        try(ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures=new ArrayList<>();
            for(int i=0;i<8;i++)futures.add(pool.submit(()->{ids.add(engine.start(def("[{\"id\":\"a\"}]"),key,System.currentTimeMillis()));return null;}));
            for(var f:futures)f.get();
        }
        assertEquals(1,ids.size());
        assertThrows(IllegalArgumentException.class,()->engine.start(def("[{\"id\":\"other\"}]"),key,System.currentTimeMillis()));
    }
    @Test void parallelCompletionsOnOneExecutionRetryUnderContention()throws Exception {
        StringBuilder steps=new StringBuilder("[");
        for(int i=0;i<12;i++)steps.append(i==0?"":",").append("{\"id\":\"p").append(i).append("\"}");
        long now=System.currentTimeMillis();
        String id=engine.start(def(steps.append("]").toString()),"contend-"+UUID.randomUUID(),now);
        List<Claim> claims=new ArrayList<>();
        for(DynamoEngine.Message m:drain(id,12))claims.add(((DynamoEngine.Claimed)engine.claimTask("w",m,now,60_000)).claim());
        try(ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> results=new ArrayList<>();
            for(Claim c:claims)results.add(pool.submit(()->engine.complete(c,c.task(),null,now+1)));
            for(var r:results)assertTrue(r.get());
        }
        assertEquals("SUCCEEDED",engine.get(id).get("status"));
    }
    @Test void largeResultsLiveInS3AndLedgerIsIdempotent()throws Exception {
        long now=System.currentTimeMillis();
        String id=engine.start(def("[{\"id\":\"a\"}]"),"s3-"+UUID.randomUUID(),now);
        Claim c=((DynamoEngine.Claimed)engine.claimTask("w",drain(id,1).get(0),now,10_000)).claim();
        assertTrue(engine.complete(c,Map.of("blob","y".repeat(5_000)),null,now+1));
        Map<String,Object> task=Json.obj(Json.obj(engine.get(id).get("tasks")).get("a"));
        assertEquals("s3",task.get("resultStoredIn"));
        DynamoLedger ledger=new DynamoLedger(engine,aws);
        String key="effect-"+UUID.randomUUID();
        assertEquals(Map.of("n",1L),ledger.apply(key,Map.of("n",1)));
        assertEquals(Map.of("n",1L),ledger.apply(key,Map.of("n",1)),"replay returns first result");
        assertThrows(IllegalArgumentException.class,()->ledger.apply(key,Map.of("n",2)));
    }
}
