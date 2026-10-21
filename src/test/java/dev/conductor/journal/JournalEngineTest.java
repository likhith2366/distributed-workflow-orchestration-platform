package dev.conductor.journal;

import static org.junit.jupiter.api.Assertions.*;

import dev.conductor.core.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournalEngineTest {
    @TempDir Path temp;
    private int dirs;
    private Path dir()throws IOException{return Files.createDirectories(temp.resolve("d"+dirs++));}
    static Map<String,Object> def(String steps) {
        return Json.obj(Json.parse("{\"name\":\"test\",\"version\":\"1\",\"steps\":"+steps+"}"));
    }
    static void rejects(org.junit.jupiter.api.function.Executable body) {
        Throwable t=assertThrows(Exception.class,body);
        assertTrue(t instanceof IllegalArgumentException||t instanceof IllegalStateException||t instanceof IOException,"unexpected "+t);
    }

    @Test void submissionIdempotencyAndPinnedDefinition()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            Map<String,Object> d=def("[{\"id\":\"a\"}]");
            String id=e.start(d,"same",0);
            assertEquals(id,e.start(d,"same",1));
            d.put("version","2");
            rejects(()->e.start(d,"same",2));
            assertEquals("1",Json.obj(e.get(id).get("definition")).get("version"));
        }
    }
    @Test void parallelBranchesAndJoin()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\"},{\"id\":\"b\"},{\"id\":\"join\",\"dependsOn\":[\"a\",\"b\"]}]"),"dag",0);
            var a=e.claim("one",1,100);var b=e.claim("two",1,100);
            assertNull(e.claim("three",1,100),"join blocked");
            e.complete(a,"a",null,2);assertNull(e.claim("three",2,100),"one branch insufficient");
            e.complete(b,"b",null,3);var join=e.claim("three",4,100);
            assertEquals("join",join.task());
            e.complete(join,"done",null,5);
            assertEquals("SUCCEEDED",e.get(id).get("status"));
        }
    }
    @Test void expiredOwnersFencedAndDurableRecovery()throws Exception {
        Path p=dir();String id;Claim old;
        try(JournalEngine e=new JournalEngine(p)){id=e.start(def("[{\"id\":\"a\"}]"),"fence",0);old=e.claim("dead",1,10);}
        try(JournalEngine e=new JournalEngine(p)) {
            assertFalse(e.complete(old,null,null,11),"expired write");
            assertFalse(e.heartbeat(old,11,100),"expired heartbeat");
            var next=e.claim("new",11,100);
            assertTrue(next.fence()>old.fence(),"monotonic fencing");
            assertFalse(e.complete(old,null,null,12),"zombie write");
            assertTrue(e.complete(next,42,null,12),"new owner");
            assertFalse(e.complete(next,42,null,13),"duplicate completion");
            assertEquals("SUCCEEDED",e.get(id).get("status"));
        }
    }
    @Test void renewalPreservesOwnershipAndStaleDueEntriesAreIgnored()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            e.start(def("[{\"id\":\"a\"}]"),"renew",0);var c=e.claim("w",1,10);
            assertTrue(e.heartbeat(c,5,100));
            // The lease-expiry entry for t=11 is now stale; the renewed lease runs to t=105.
            assertNull(e.claim("other",12,100),"no takeover");
            assertTrue(e.complete(c,1,null,20));
        }
    }
    @Test void retryBackoffSurvivesRestart()throws Exception {
        Path p=dir();
        try(JournalEngine e=new JournalEngine(p)) {
            e.start(def("[{\"id\":\"a\",\"backoffMs\":50}]"),"retry",0);
            var c=e.claim("w",0,100);e.complete(c,null,"transient",1);
        }
        try(JournalEngine e=new JournalEngine(p)) {
            assertNull(e.claim("w",50,100),"not early");
            var c=e.claim("w",51,100);assertEquals(2,c.attempt());
            e.complete(c,1,null,52);
        }
    }
    @Test void approvalAndDurableTimer()throws Exception {
        Path p=dir();String id;
        try(JournalEngine e=new JournalEngine(p)) {
            id=e.start(def("[{\"id\":\"approve\",\"action\":\"approval\"},{\"id\":\"timer\",\"action\":\"wait\",\"delayMs\":100,\"dependsOn\":[\"approve\"]}]"),"timer",0);
            assertNull(e.claim("w",1000,100),"approval blocked");
            e.signal(id,"approve",1000);var c=e.claim("w",1000,100);e.complete(c,1,null,1001);
        }
        try(JournalEngine e=new JournalEngine(p)) {
            assertNull(e.claim("w",1100,100),"timer not early");
            var c=e.claim("w",1101,100);assertEquals("timer",c.task());
            e.complete(c,1,null,1102);
            assertEquals("SUCCEEDED",e.get(id).get("status"));
        }
    }
    @Test void reverseCompensationOrder()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\",\"compensate\":\"ledger\"},{\"id\":\"b\",\"compensate\":\"ledger\",\"dependsOn\":[\"a\"]},{\"id\":\"c\",\"action\":\"fail\",\"maxAttempts\":1,\"dependsOn\":[\"b\"]}]"),"saga",0);
            e.complete(e.claim("w",0,100),1,null,1);
            e.complete(e.claim("w",2,100),2,null,3);
            e.complete(e.claim("w",4,100),null,"permanent",5);
            var b=e.claim("w",6,100);assertEquals("$undo:b",b.task());e.complete(b,1,null,7);
            var a=e.claim("w",8,100);assertEquals("$undo:a",a.task());e.complete(a,1,null,9);
            assertEquals("COMPENSATED",e.get(id).get("status"));
        }
    }
    @Test void partialParallelFailureDrainsRunningEffectsBeforeUndo()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\",\"compensate\":\"ledger\"},{\"id\":\"b\",\"maxAttempts\":1}]"),"partial",0);
            var a=e.claim("w1",0,100);var b=e.claim("w2",0,100);
            e.complete(b,null,"fail",1);
            assertEquals("FAILING",e.get(id).get("status"));
            e.complete(a,1,null,2);
            var undo=e.claim("w",3,100);assertEquals("$undo:a",undo.task(),"late success compensated");
            e.complete(undo,1,null,4);
            assertEquals("COMPENSATED",e.get(id).get("status"));
        }
    }
    @Test void compensationFailureIsExplicit()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\",\"compensate\":\"fail\"},{\"id\":\"b\",\"maxAttempts\":1,\"dependsOn\":[\"a\"]}]"),"badundo",0);
            e.complete(e.claim("w",0,100),1,null,1);
            e.complete(e.claim("w",2,100),null,"failed",3);
            for(int i=0;i<3;i++){var c=e.claim("w",1000+i*1000,100);e.complete(c,null,"undo failed",1001+i*1000);}
            assertEquals("COMPENSATION_FAILED",e.get(id).get("status"));
        }
    }
    @Test void leaseExhaustionTerminatesWorkflow()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\",\"maxAttempts\":1}]"),"exhaust",0);
            e.claim("dead",0,10);assertNull(e.claim("other",10,10));
            assertEquals("FAILED",e.get(id).get("status"));
        }
    }
    @Test void cancelFencesInFlightWorker()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\"}]"),"cancel",0);var c=e.claim("w",0,100);
            e.cancel(id,1);assertFalse(e.complete(c,1,null,2),"cancel rejects write");
            assertNull(e.claim("w",2,100),"no future work");
        }
    }
    @Test void effectBeforeCompletionCrashDeduplicatesDownstream()throws Exception {
        Path p=dir();String id;
        try(JournalEngine e=new JournalEngine(p);JournalLedger ledger=new JournalLedger(p)) {
            id=e.start(def("[{\"id\":\"a\",\"action\":\"ledger\"}]"),"effect",0);
            Activities.execute(e.claim("dead",0,10),ledger);
        }
        try(JournalEngine e=new JournalEngine(p);JournalLedger ledger=new JournalLedger(p)) {
            var c=e.claim("alive",10,100);
            e.complete(c,Activities.execute(c,ledger),null,11);
            assertEquals(1,ledger.size(),"one downstream effect");
            assertEquals("SUCCEEDED",e.get(id).get("status"));
        }
    }
    @Test void concurrentClaimsHaveASingleWinner()throws Exception {
        try(JournalEngine e=new JournalEngine(dir());ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            e.start(def("[{\"id\":\"a\"}]"),"race",0);
            List<Future<Claim>> results=new ArrayList<>();
            for(int i=0;i<64;i++){String owner="w"+i;results.add(pool.submit(()->e.claim(owner,0,100)));}
            int claims=0;for(var r:results)if(r.get()!=null)claims++;
            assertEquals(1,claims);
        }
    }
    @Test void completeAndClaimHandsOffInOneCommit()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\"},{\"id\":\"b\",\"dependsOn\":[\"a\"]}]"),"handoff",0);
            Claim a=e.claim("w",1,100);
            long flushes=Json.num(Json.obj(e.stats().get("journal")),"flushes",0);
            LocalEngine.Handoff h=e.completeAndClaim(a,1,null,2,"w",100);
            assertTrue(h.accepted());assertEquals("b",h.next().task());
            assertEquals(flushes+1,Json.num(Json.obj(e.stats().get("journal")),"flushes",0),"one durable flush for both transitions");
            assertFalse(e.completeAndClaim(a,1,null,3,"w",100).accepted(),"stale completion still rejected");
            assertTrue(e.complete(h.next(),2,null,4));
            assertEquals("SUCCEEDED",e.get(id).get("status"));
        }
    }
    @Test void statsTrackCommittedStatusCounts()throws Exception {
        try(JournalEngine e=new JournalEngine(dir())) {
            String id=e.start(def("[{\"id\":\"a\"}]"),"s1",0);
            e.start(def("[{\"id\":\"approve\",\"action\":\"approval\"}]"),"s2",0);
            e.complete(e.claim("w",0,100),1,null,1);
            Map<String,Object> stats=e.stats();
            assertEquals(2,((Number)stats.get("executions")).intValue());
            assertEquals(1L,stats.get("active"));
            assertEquals(Map.of("RUNNING",1L,"SUCCEEDED",1L),stats.get("statuses"));
            assertEquals(id,Json.obj(e.list(2).get(1)).get("id"),"newest first");
            assertEquals(1,e.list(1).size());
        }
    }
    @Test void incompleteWalTailTruncatesSafely()throws Exception {
        Path p=dir();String id;
        try(JournalEngine e=new JournalEngine(p)){id=e.start(def("[{\"id\":\"a\"}]"),"tail",0);}
        long original=Files.size(p.resolve("events.wal"));
        Files.write(p.resolve("events.wal"),ByteBuffer.allocate(6).putInt(100).putShort((short)42).array(),StandardOpenOption.APPEND);
        try(JournalEngine e=new JournalEngine(p)){assertNotNull(e.get(id));}
        assertEquals(original,Files.size(p.resolve("events.wal")));
    }
    @Test void completeChecksumCorruptionFailsClosed()throws Exception {
        Path p=dir();
        try(JournalEngine e=new JournalEngine(p)){e.start(def("[{\"id\":\"a\"}]"),"corrupt",0);}
        byte[] bytes=Files.readAllBytes(p.resolve("events.wal"));bytes[bytes.length-1]^=1;Files.write(p.resolve("events.wal"),bytes);
        rejects(()->{try(JournalEngine ignored=new JournalEngine(p)){}});
    }
    @Test void replayedProjectionIsByteForByteEqual()throws Exception {
        Path p=dir();String id,snapshot;
        try(JournalEngine e=new JournalEngine(p)) {
            id=e.start(def("[{\"id\":\"a\"}]"),"replay",0);e.complete(e.claim("w",1,100),Map.of("value",7),null,2);
            snapshot=Json.write(e.get(id));
        }
        try(JournalEngine e=new JournalEngine(p)){assertEquals(snapshot,Json.write(e.get(id)));}
    }
    @Test void completionAndLedgerCannotLeakMutableValues()throws Exception {
        Path p=dir();
        try(JournalEngine e=new JournalEngine(p);JournalLedger ledger=new JournalLedger(p)) {
            String id=e.start(def("[{\"id\":\"a\"}]"),"alias",0);
            Map<String,Object> value=new LinkedHashMap<>(Map.of("nested",new ArrayList<>(List.of("original"))));
            e.complete(e.claim("w",0,100),value,null,1);
            Json.arr(value.get("nested")).set(0,"mutated");
            Map<String,Object> task=Json.obj(Json.obj(e.get(id).get("tasks")).get("a"));
            assertEquals("original",Json.arr(Json.obj(task.get("result")).get("nested")).get(0));
            Json.arr(Json.obj(Json.obj(Json.obj(e.get(id).get("tasks")).get("a")).get("result")).get("nested")).set(0,"reader mutation");
            assertEquals("original",Json.arr(Json.obj(Json.obj(Json.obj(e.get(id).get("tasks")).get("a")).get("result")).get("nested")).get(0),"reads are copies");
            Object returned=ledger.apply("key",value);
            Json.arr(Json.obj(returned).get("nested")).set(0,"leak");
            assertEquals(Json.write(value),Json.write(ledger.apply("key",value)));
        }
    }
    @Test void concurrentGroupCommittedStartsSurviveReplay()throws Exception {
        Path p=dir();Set<String> ids=new HashSet<>();
        try(JournalEngine e=new JournalEngine(p);ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> futures=new ArrayList<>();
            for(int i=0;i<100;i++){String key="group-"+i;futures.add(pool.submit(()->e.start(def("[{\"id\":\"a\"}]"),key,0)));}
            for(Future<String> f:futures)ids.add(f.get());
            assertEquals(100,ids.size());
        }
        try(JournalEngine e=new JournalEngine(p)){for(String id:ids)assertEquals("RUNNING",e.get(id).get("status"));}
    }
    @Test void awaitWorkWakesOnNewlyDueTask()throws Exception {
        try(JournalEngine e=new JournalEngine(dir());ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            long generation=e.workGeneration();
            Future<Long> waited=pool.submit(()->{long t=System.nanoTime();e.awaitWork(generation,5_000);return (System.nanoTime()-t)/1_000_000;});
            Thread.sleep(50);
            e.start(def("[{\"id\":\"a\"}]"),"wake",System.currentTimeMillis());
            assertTrue(waited.get(5,TimeUnit.SECONDS)<2_000,"woken by commit, not by timeout");
        }
    }

    @Test void groupCommitGatesWritesAndReadsOnFlush()throws Exception {
        CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        AtomicInteger state=new AtomicInteger();
        try(DurableDispatcher d=new DurableDispatcher("test-flush",()->{
            entered.countDown();
            try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("test timed out");}
            catch(InterruptedException e){throw new IOException(e);}
        });ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Integer> write=pool.submit(()->d.call(state::incrementAndGet));
            assertTrue(entered.await(5,TimeUnit.SECONDS));
            Future<Integer> read=pool.submit(()->d.read(state::get));
            try {
                assertThrows(TimeoutException.class,()->write.get(30,TimeUnit.MILLISECONDS),"acknowledged before flush");
                assertFalse(read.isDone(),"read exposed speculative state");
            } finally{release.countDown();}
            assertEquals(1,write.get());assertEquals(1,read.get());
        }
    }
    @Test void failedGroupFlushPoisonsSubsequentReadsAndWrites()throws Exception {
        try(DurableDispatcher d=new DurableDispatcher("test-poison",()->{throw new IOException("injected flush failure");})) {
            rejects(()->d.call(()->42));
            rejects(()->d.call(()->43));
            assertThrows(java.io.UncheckedIOException.class,()->d.read(()->44));
            assertFalse(d.healthy());
        }
    }
    @Test void batchedWalKeepsIndependentlyRecoverableFrames()throws Exception {
        Path file=dir().resolve("batch.wal");List<Map<String,Object>> replay=new ArrayList<>();
        try(Journal journal=new Journal(file,replay::add)) {
            journal.appendBatch(List.of(Map.of("id",1),Map.of("id",2),Map.of("id",3)));
            assertEquals(1,Json.num(journal.stats(),"flushes",0),"one flush for three records");
        }
        try(Journal ignored=new Journal(file,replay::add)){}
        assertEquals(3,replay.size());
        byte[] content=Files.readAllBytes(file);
        Files.write(file,Arrays.copyOf(content,content.length-2));
        replay.clear();
        try(Journal ignored=new Journal(file,replay::add)){}
        assertEquals(2,replay.size(),"complete prefix retained after torn last frame");
    }
}
