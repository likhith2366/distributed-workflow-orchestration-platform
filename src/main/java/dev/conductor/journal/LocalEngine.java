package dev.conductor.journal;

import dev.conductor.core.*;
import java.io.IOException;

/** A journal-backed engine that workers pull from: claim, handoff, and commit-signalled waiting. */
public interface LocalEngine extends WorkflowEngine {
    /** Outcome of {@link #completeAndClaim}: whether the completion was accepted, and the next lease if any. */
    record Handoff(boolean accepted,Claim next) {}
    /** Completes a task and claims the worker's next one, sharing one durable flush where possible. */
    Handoff completeAndClaim(Claim c,Object result,String error,long now,String owner,long leaseMs)throws IOException;
    long workGeneration();
    /** Blocks until a commit may have produced due work, the next timer is due, or {@code maxWaitMs} passes. */
    void awaitWork(long seenGeneration,long maxWaitMs)throws InterruptedException;
    /**
     * Claims due work, or waits up to {@code maxWaitMs} for some; returns null if none arrived.
     * The default wakes on commit signals and then claims, which costs a second durable flush.
     */
    default Claim claimOrWait(String owner,long leaseMs,long maxWaitMs)throws IOException,InterruptedException {
        long generation=workGeneration();
        Claim claim=claim(owner,System.currentTimeMillis(),leaseMs);
        if(claim!=null||maxWaitMs<=0)return claim;
        awaitWork(generation,maxWaitMs);
        return claim(owner,System.currentTimeMillis(),leaseMs);
    }
}
