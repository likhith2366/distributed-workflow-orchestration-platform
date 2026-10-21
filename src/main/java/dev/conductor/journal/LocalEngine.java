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
}
