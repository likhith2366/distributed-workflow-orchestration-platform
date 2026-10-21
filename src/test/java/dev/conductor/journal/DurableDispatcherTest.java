package dev.conductor.journal;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class DurableDispatcherTest {
    @org.junit.jupiter.api.BeforeAll static void pipelined(){System.setProperty("conductor.pipelinedCommit","true");}
    @org.junit.jupiter.api.AfterAll static void reset(){System.clearProperty("conductor.pipelinedCommit");}
    /** A log whose first force blocks until released, recording the size of every forced batch. */
    static final class GatedLog implements DurableDispatcher.Log {
        final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        final List<Integer> forced=Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger staged=new AtomicInteger();
        volatile boolean failNext;
        public DurableDispatcher.Staged stage(){staged.incrementAndGet();return new DurableDispatcher.Staged(new byte[]{1},1,null);}
        public void force(byte[] frames,int records)throws IOException {
            if(failNext)throw new IOException("injected");
            entered.countDown();
            try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("timed out");}
            catch(InterruptedException e){throw new IOException(e);}
            forced.add(records);
        }
    }

    @Test void executionOverlapsAnInFlightFlushButRepliesWaitForTheirOwn()throws Exception {
        GatedLog log=new GatedLog();
        AtomicInteger state=new AtomicInteger();
        try(DurableDispatcher d=new DurableDispatcher("overlap",log);ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Integer> first=pool.submit(()->d.call(state::incrementAndGet));
            assertTrue(log.entered.await(5,TimeUnit.SECONDS),"first batch is flushing");
            List<Future<Integer>> later=new ArrayList<>();
            for(int i=0;i<5;i++)later.add(pool.submit(()->d.call(state::incrementAndGet)));
            long deadline=System.nanoTime()+5_000_000_000L;
            while(state.get()<6&&System.nanoTime()<deadline)Thread.sleep(1);
            assertEquals(6,state.get(),"commands executed while the previous batch was in fsync");
            assertFalse(first.isDone(),"no reply before its own flush");
            for(Future<Integer> f:later)assertFalse(f.isDone(),"no reply before its own flush");
            log.release.countDown();
            assertEquals(1,first.get());
            for(Future<Integer> f:later)f.get(5,TimeUnit.SECONDS);
            assertEquals(List.of(1,5),log.forced,"the flusher took everything accumulated as one batch");
        }
    }
    @Test void flushFailurePoisonsEveryLaterCommand()throws Exception {
        GatedLog log=new GatedLog();log.failNext=true;
        try(DurableDispatcher d=new DurableDispatcher("poison",log)) {
            assertThrows(IOException.class,()->d.call(()->1));
            assertFalse(d.healthy());
            assertThrows(IOException.class,()->d.call(()->2));
            assertThrows(java.io.UncheckedIOException.class,()->d.read(()->3));
        }
    }
    @Test void backpressureBoundsTheOpenBatch()throws Exception {
        System.setProperty("conductor.batchSize","4");
        GatedLog log=new GatedLog();
        try(DurableDispatcher d=new DurableDispatcher("bounded",log);ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> all=new ArrayList<>();
            all.add(pool.submit(()->d.call(()->0)));
            assertTrue(log.entered.await(5,TimeUnit.SECONDS));
            for(int i=1;i<=10;i++){int n=i;all.add(pool.submit(()->d.call(()->n)));}
            Thread.sleep(100);
            assertTrue(log.staged.get()<=1+4+1,"executor stops once the open batch is full: "+log.staged.get());
            log.release.countDown();
            for(Future<Integer> f:all)f.get(5,TimeUnit.SECONDS);
            synchronized(log.forced){for(int size:log.forced)assertTrue(size<=4,"batch within cap: "+log.forced);}
        } finally {System.clearProperty("conductor.batchSize");}
    }
}
