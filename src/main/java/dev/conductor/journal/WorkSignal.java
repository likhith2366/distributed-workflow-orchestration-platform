package dev.conductor.journal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

/**
 * Wakes idle workers when a commit makes work due. Shared by all shards of a partitioned journal so
 * one wait covers every shard. Callers read {@link #generation()} before trying to claim and pass it
 * to {@link #await}, which returns immediately if any commit signalled in between (no lost wakeups).
 */
public final class WorkSignal {
    private final ReentrantLock lock=new ReentrantLock();
    private final Condition work=lock.newCondition();
    private long generation;
    public long generation() {
        lock.lock();
        try{return generation;}finally{lock.unlock();}
    }
    void signal() {
        lock.lock();
        try{generation++;work.signalAll();}finally{lock.unlock();}
    }
    public void await(long seenGeneration,long maxWaitMs)throws InterruptedException {
        if(maxWaitMs<=0)return;
        long nanos=TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        lock.lock();
        try{while(generation==seenGeneration&&nanos>0)nanos=work.awaitNanos(nanos);}
        finally{lock.unlock();}
    }
}
