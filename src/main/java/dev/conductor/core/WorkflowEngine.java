package dev.conductor.core;

import java.io.IOException;
import java.util.*;

/**
 * Durable workflow operations. Every mutating call returns only after its transition is durable.
 * Times are supplied by the caller so that lease validity is decided by the process holding state.
 */
public interface WorkflowEngine extends AutoCloseable {
    String start(Map<String,Object> definition,String key,long now)throws IOException;
    /** Claims the next due task, or returns null. Queue-dispatched backends do not support scanning. */
    Claim claim(String owner,long now,long leaseMs)throws IOException;
    boolean heartbeat(Claim c,long now,long leaseMs)throws IOException;
    boolean complete(Claim c,Object result,String error,long now)throws IOException;
    void signal(String id,String step,long now)throws IOException;
    void cancel(String id,long now)throws IOException;
    /** @throws NoSuchElementException when the execution does not exist */
    Map<String,Object> get(String id)throws IOException;
    List<Object> list(int limit)throws IOException;
    /** The execution once terminal, or its current state after {@code timeoutMs}. Polls by default. */
    default Map<String,Object> awaitTerminal(String id,long timeoutMs)throws IOException,InterruptedException {
        long deadline=System.currentTimeMillis()+timeoutMs,pause=20;
        while(true) {
            Map<String,Object> run=get(id);
            if(Workflow.terminal(Workflow.status(run))||System.currentTimeMillis()>=deadline)return run;
            Thread.sleep(Math.min(pause,Math.max(1,deadline-System.currentTimeMillis())));
            pause=Math.min(250,pause*2);
        }
    }
    Map<String,Object> stats()throws IOException;
    /** False once a durability failure requires a restart. */
    boolean healthy();
    String backend();
    @Override void close()throws IOException;
}
