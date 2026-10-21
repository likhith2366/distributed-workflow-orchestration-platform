package dev.conductor.aws;

import java.util.concurrent.*;

/** Background sweeper that delivers outbox items left behind by crashed processes, and due long timers. */
public final class OutboxRelay implements AutoCloseable {
    private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("outbox-relay").factory());
    public OutboxRelay(DynamoEngine engine,long intervalMs,long graceMs) {
        scheduler.scheduleWithFixedDelay(()->{
            try{while(engine.relayOnce(System.currentTimeMillis(),graceMs)>0&&!Thread.currentThread().isInterrupted()){}}
            catch(RuntimeException e){System.err.println("Outbox relay retrying: "+e.getMessage());}
        },intervalMs,intervalMs,TimeUnit.MILLISECONDS);
    }
    @Override public void close() {
        scheduler.shutdownNow();
        try{scheduler.awaitTermination(5,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}
    }
}
