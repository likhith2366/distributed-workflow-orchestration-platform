package dev.conductor.journal;

import static org.junit.jupiter.api.Assertions.*;

import dev.conductor.core.*;
import dev.conductor.worker.EmbeddedWorkers;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionedJournalTest {
    @TempDir Path dir;
    static Map<String,Object> def(String steps){return JournalEngineTest.def(steps);}

    @Test void keysAndIdsRouteToOneShardAndSurviveReopen()throws Exception {
        Map<String,String> ids=new HashMap<>();
        try(PartitionedJournal.Store s=PartitionedJournal.open(dir,4)) {
            for(int i=0;i<40;i++) {
                String key="k"+i,id=s.engine().start(def("[{\"id\":\"a\"}]"),key,i);
                assertEquals(id,s.engine().start(def("[{\"id\":\"a\"}]"),key,i),"idempotent within the owning shard");
                assertEquals(PartitionedJournal.shardOf(key,4),PartitionedJournal.shardOf(id,4),"id routes to key's shard");
                ids.put(key,id);
            }
            assertEquals(40L,s.engine().stats().get("executions"));
        }
        for(int i=0;i<4;i++)assertTrue(Files.exists(dir.resolve("shard-"+i).resolve("events.wal")));
        try(PartitionedJournal.Store s=PartitionedJournal.open(dir,4)) {
            for(var e:ids.entrySet())assertEquals("RUNNING",s.engine().get(e.getValue()).get("status"));
            List<Object> latest=s.engine().list(5);
            assertEquals(5,latest.size());
            assertEquals(ids.get("k39"),Json.obj(latest.get(0)).get("id"),"merged newest-first across shards");
        }
    }
    @Test void reopeningWithADifferentShardCountFails()throws Exception {
        try(PartitionedJournal.Store ignored=PartitionedJournal.open(dir,4)){}
        assertThrows(IOException.class,()->PartitionedJournal.open(dir,2));
        assertThrows(IOException.class,()->PartitionedJournal.open(dir,1));
        Path single=Files.createDirectories(dir.resolve("single"));
        try(PartitionedJournal.Store ignored=PartitionedJournal.open(single,1)){}
        assertTrue(Files.exists(single.resolve("events.wal")),"one shard keeps the original layout");
        assertThrows(IOException.class,()->PartitionedJournal.open(single,4));
    }
    @Test void workersStealAcrossShardsAndFencingStillHolds()throws Exception {
        try(PartitionedJournal.Store s=PartitionedJournal.open(dir,4)) {
            LocalEngine e=s.engine();
            List<String> ids=new ArrayList<>();
            for(int i=0;i<8;i++)ids.add(e.start(def("[{\"id\":\"a\"},{\"id\":\"b\",\"dependsOn\":[\"a\"]}]"),"steal"+i,0));
            // One owner drains every shard; its handoff keeps claiming until nothing is due.
            Claim c=e.claim("solo",1,100);int steps=0;
            while(c!=null){steps++;c=e.completeAndClaim(c,steps,null,2,"solo",100).next();}
            assertEquals(16,steps);
            for(String id:ids)assertEquals("SUCCEEDED",e.get(id).get("status"));
            String id=e.start(def("[{\"id\":\"a\"}]"),"fence",0);
            Claim old=e.claim("dead",10,10),next=e.claim("live",20,100);
            assertEquals(id,next.execution());
            assertFalse(e.complete(old,1,null,21));
            assertTrue(e.complete(next,1,null,21));
        }
    }
    @Test void embeddedWorkersCompleteConcurrentLoadExactlyOncePerEffect()throws Exception {
        try(PartitionedJournal.Store s=PartitionedJournal.open(dir,4);
            EmbeddedWorkers workers=new EmbeddedWorkers(s.engine(),s.ledger(),8,3000);
            ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> futures=new ArrayList<>();
            for(int i=0;i<200;i++){String key="load"+i;futures.add(pool.submit(()->{
                String id=s.engine().start(def("[{\"id\":\"a\",\"action\":\"ledger\"},{\"id\":\"b\",\"action\":\"ledger\",\"dependsOn\":[\"a\"]}]"),key,System.currentTimeMillis());
                return Json.str(s.engine().awaitTerminal(id,10_000),"status","");
            }));}
            for(Future<String> f:futures)assertEquals("SUCCEEDED",f.get());
            assertEquals(400,s.ledger().size());
            assertEquals(Map.of("SUCCEEDED",200L),s.engine().stats().get("statuses"));
        }
    }
}
