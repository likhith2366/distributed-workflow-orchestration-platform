package dev.conductor.worker;

import dev.conductor.core.*;
import dev.conductor.journal.LocalEngine;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** In-process virtual-thread workers for the journal backend. Idle workers block on commit signals. */
public final class EmbeddedWorkers implements AutoCloseable {
    private final ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("heartbeats").factory());
    private final AtomicBoolean stopping=new AtomicBoolean();
    private final LongAdder completed=new LongAdder(),rejected=new LongAdder(),errors=new LongAdder();
    private final LatencyRecorder dispatch=new LatencyRecorder(65_536);
    // Step anatomy: activity (incl. its downstream effect) vs the durable completion+handoff commit.
    private final LatencyRecorder activityTime=new LatencyRecorder(65_536),commitTime=new LatencyRecorder(65_536),effectTime=new LatencyRecorder(65_536);
    private final LocalEngine engine;
    private final EffectLedger ledger;
    private final long leaseMs;
    public EmbeddedWorkers(LocalEngine engine,EffectLedger ledger,int count,long leaseMs) {
        if(count<0||count>256||leaseMs<300)throw new IllegalArgumentException("Invalid worker configuration");
        this.engine=engine;this.leaseMs=leaseMs;
        // Times each downstream effect call, so step anatomy can separate it from activity work.
        this.ledger=ledger==null?null:new EffectLedger() {
            public Object apply(String key,Object value)throws java.io.IOException {
                long t=System.nanoTime();
                try{return ledger.apply(key,value);}finally{effectTime.recordMicros(System.nanoTime()-t);}
            }
            public long size()throws java.io.IOException{return ledger.size();}
            public void close()throws java.io.IOException{ledger.close();}
        };
        for(int i=0;i<count;i++){String owner="worker-"+i;pool.submit(()->loop(owner));}
    }
    private void loop(String owner) {
        Claim next=null;
        while(!stopping.get())try {
            Claim claim=next;next=null;
            if(claim==null) {
                long generation=engine.workGeneration();
                claim=engine.claim(owner,System.currentTimeMillis(),leaseMs);
                if(claim==null){engine.awaitWork(generation,50);continue;}
            }
            next=run(owner,claim);
        } catch(InterruptedException e){Thread.currentThread().interrupt();return;}
          catch(Exception e) {
              if(stopping.get())return;
              errors.increment();System.err.println("Worker error: "+e.getMessage());
              try{Thread.sleep(100);}catch(InterruptedException ex){return;}
          }
    }
    /** Executes one lease and returns the next one, claimed in the same commit as this completion. */
    private Claim run(String owner,Claim claim)throws Exception {
        dispatch.record(System.currentTimeMillis()-claim.readyAt());
        ScheduledFuture<?> heartbeat=heartbeats.scheduleAtFixedRate(()->{
            try{if(!engine.heartbeat(claim,System.currentTimeMillis(),leaseMs))rejected.increment();}
            catch(Exception e){errors.increment();}
        },leaseMs/3,leaseMs/3,TimeUnit.MILLISECONDS);
        Object result=null;String failure=null;
        long started=System.nanoTime();
        try{result=Activities.execute(claim,ledger);}
        catch(InterruptedException e){throw e;}
        catch(Exception e){failure=Objects.toString(e.getMessage(),e.getClass().getSimpleName());}
        finally{heartbeat.cancel(false);}
        long executed=System.nanoTime();
        LocalEngine.Handoff handoff=engine.completeAndClaim(claim,result,failure,System.currentTimeMillis(),owner,leaseMs);
        activityTime.recordMicros(executed-started);commitTime.recordMicros(System.nanoTime()-executed);
        if(handoff.accepted())completed.increment();else rejected.increment();
        return handoff.next();
    }
    public Map<String,Object> stats() {
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("mode","embedded");m.put("completions",completed.sum());m.put("fenceRejections",rejected.sum());
        m.put("workerErrors",errors.sum());m.putAll(dispatch.snapshot("dispatch"));
        m.putAll(activityTime.snapshot("activityMicros"));m.putAll(commitTime.snapshot("commitMicros"));m.putAll(effectTime.snapshot("effectMicros"));
        return m;
    }
    @Override public void close() {
        stopping.set(true);pool.shutdownNow();heartbeats.shutdownNow();
        try{pool.awaitTermination(5,TimeUnit.SECONDS);heartbeats.awaitTermination(5,TimeUnit.SECONDS);}
        catch(InterruptedException e){Thread.currentThread().interrupt();}
    }
}
