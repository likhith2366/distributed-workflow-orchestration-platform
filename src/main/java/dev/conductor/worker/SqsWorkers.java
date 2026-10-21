package dev.conductor.worker;

import dev.conductor.aws.DynamoEngine;
import dev.conductor.aws.DynamoEngine.*;
import dev.conductor.core.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

/**
 * Stateless workers for the DynamoDB backend. A message is only a hint to attempt a conditional
 * claim; the message is deleted once the claim (and its durable lease-check outbox entry) commits,
 * so a worker crash mid-activity is recovered by the lease-check message, not by SQS visibility.
 */
public final class SqsWorkers implements AutoCloseable {
    private final ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("sqs-heartbeats").factory());
    private final AtomicBoolean stopping=new AtomicBoolean();
    private final LongAdder completed=new LongAdder(),rejected=new LongAdder(),errors=new LongAdder(),requeued=new LongAdder(),dropped=new LongAdder();
    private final LatencyRecorder dispatch=new LatencyRecorder(65_536);
    private final DynamoEngine engine;
    private final EffectLedger ledger;
    private final SqsClient sqs;
    private final String queueUrl;
    private final long leaseMs;
    public SqsWorkers(DynamoEngine engine,EffectLedger ledger,SqsClient sqs,String queueUrl,int count,long leaseMs) {
        if(count<0||count>256||leaseMs<1000)throw new IllegalArgumentException("Invalid worker configuration");
        this.engine=engine;this.ledger=ledger;this.sqs=sqs;this.queueUrl=queueUrl;this.leaseMs=leaseMs;
        String process=UUID.randomUUID().toString();
        for(int i=0;i<count;i++){String owner=process+"/"+i;pool.submit(()->loop(owner));}
    }
    private void loop(String owner) {
        while(!stopping.get()) {
            try {
                List<software.amazon.awssdk.services.sqs.model.Message> messages=sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(5).build()).messages();
                for(var message:messages)handle(owner,message);
            } catch(Exception e) {
                if(stopping.get()||Thread.currentThread().isInterrupted())return;
                errors.increment();System.err.println("SQS worker retry: "+e.getMessage());
                try{Thread.sleep(250);}catch(InterruptedException ex){return;}
            }
        }
    }
    private void handle(String owner,software.amazon.awssdk.services.sqs.model.Message raw)throws Exception {
        DynamoEngine.Message m;
        try{m=DynamoEngine.Message.parse(raw.body());}
        catch(IllegalArgumentException e){System.err.println("Dropping malformed message: "+e.getMessage());delete(raw);dropped.increment();return;}
        long now=System.currentTimeMillis();
        TaskOutcome outcome=engine.claimTask(owner,m,now,leaseMs);
        switch(outcome) {
            case Gone g -> {delete(raw);dropped.increment();}
            case NotDue d -> {engine.requeue(m,d.dueAt(),now);delete(raw);requeued.increment();}
            case Claimed c -> {delete(raw);run(c.claim());}
        }
    }
    private void run(Claim claim)throws Exception {
        dispatch.record(System.currentTimeMillis()-claim.readyAt());
        ScheduledFuture<?> heartbeat=heartbeats.scheduleAtFixedRate(()->pool.submit(()->{
            try{if(!engine.heartbeat(claim,System.currentTimeMillis(),leaseMs))rejected.increment();}
            catch(Exception e){errors.increment();}
        }),leaseMs/3,leaseMs/3,TimeUnit.MILLISECONDS);
        Object result=null;String failure=null;
        try{result=Activities.execute(claim,ledger);}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw e;}
        catch(Exception e){failure=Objects.toString(e.getMessage(),e.getClass().getSimpleName());}
        finally{heartbeat.cancel(false);}
        if(engine.complete(claim,result,failure,System.currentTimeMillis()))completed.increment();else rejected.increment();
    }
    private void delete(software.amazon.awssdk.services.sqs.model.Message raw) {
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(raw.receiptHandle()).build());
    }
    public Map<String,Object> stats() {
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("mode","sqs");m.put("completions",completed.sum());m.put("fenceRejections",rejected.sum());
        m.put("workerErrors",errors.sum());m.put("requeued",requeued.sum());m.put("staleMessagesDropped",dropped.sum());
        m.putAll(dispatch.snapshot("dispatch"));
        return m;
    }
    @Override public void close() {
        stopping.set(true);pool.shutdown();heartbeats.shutdownNow();
        try{if(!pool.awaitTermination(8,TimeUnit.SECONDS))pool.shutdownNow();}
        catch(InterruptedException e){Thread.currentThread().interrupt();}
    }
}
