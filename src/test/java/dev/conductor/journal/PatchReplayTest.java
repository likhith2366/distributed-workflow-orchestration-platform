package dev.conductor.journal;

import static org.junit.jupiter.api.Assertions.*;

import dev.conductor.core.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PatchReplayTest {
    @TempDir Path dir;
    static final String DEF="""
        [{"id":"pay","action":"ledger","compensate":"ledger","maxAttempts":4,"backoffMs":5},
         {"id":"review","action":"approval","dependsOn":["pay"]},
         {"id":"left","dependsOn":["review"],"maxAttempts":3},
         {"id":"right","dependsOn":["review"],"compensate":"echo","maxAttempts":2},
         {"id":"timer","action":"wait","delayMs":20,"dependsOn":["left","right"]},
         {"id":"ship","dependsOn":["timer"],"maxAttempts":2}]""";

    @Test void randomizedTransitionsReplayToByteIdenticalProjections()throws Exception {
        Random random=new Random(20261020);
        Map<String,String> snapshot=new TreeMap<>();
        long now=0;
        try(JournalEngine e=new JournalEngine(dir)) {
            List<String> ids=new ArrayList<>();
            for(int i=0;i<30;i++)ids.add(e.start(JournalEngineTest.def(DEF),"r"+i,now));
            for(int step=0;step<3000;step++) {
                now+=random.nextInt(15);
                String id=ids.get(random.nextInt(ids.size()));
                switch(random.nextInt(10)) {
                    case 0 -> {try{e.signal(id,"review",now);}catch(IllegalArgumentException ignored){}}
                    case 1 -> {if(random.nextInt(20)==0)e.cancel(id,now);}
                    default -> {
                        Claim c=e.claim("w"+random.nextInt(4),now,30);
                        if(c==null)break;
                        int fate=random.nextInt(10);
                        if(fate==0)break;                                         // abandon: lease expires, reclaim later
                        if(fate==1)e.heartbeat(c,now+1,30);
                        String error=fate<=3?"boom "+step:null;
                        Object result=error==null?Map.of("step",step,"nested",List.of(Map.of("k",fate))):null;
                        e.complete(c,result,error,now+2);
                    }
                }
            }
            for(String id:ids)snapshot.put(id,Json.write(e.get(id)));
            long full=ids.stream().filter(id->Workflow.terminal(Json.str(snapshot.containsKey(id)?Json.obj(Json.parse(snapshot.get(id))):Map.of(),"status",""))).count();
            assertTrue(full>0,"workload reached terminal states");
            long bytes=Json.num(Json.obj(e.stats().get("journal")),"bytes",0),records=Json.num(Json.obj(e.stats().get("journal")),"records",0);
            if(!Boolean.getBoolean("conductor.fullProjectionRecords"))
                assertTrue(bytes/records<600,"patch records are small: "+bytes/records+" bytes/record");
        }
        try(JournalEngine e=new JournalEngine(dir)) {
            for(var entry:snapshot.entrySet())assertEquals(entry.getValue(),Json.write(e.get(entry.getKey())),"replayed "+entry.getKey());
        }
    }
    @Test void walsOfFullProjectionsFromEarlierVersionsStillReplayAndContinue()throws Exception {
        Map<String,Object> run=Workflow.newRun(JournalEngineTest.def("[{\"id\":\"a\"},{\"id\":\"b\",\"dependsOn\":[\"a\"]}]"),"legacy","legacy-id",0);
        try(Journal journal=new Journal(dir.resolve("events.wal"),r->{})) {
            Map<String,Object> record=new LinkedHashMap<>();
            record.put("sequence",1L);record.put("type","ExecutionStarted");record.put("at",0L);record.put("execution",run);
            journal.appendBatch(List.of(record));
        }
        String afterPatches;
        try(JournalEngine e=new JournalEngine(dir)) {
            assertEquals("RUNNING",e.get("legacy-id").get("status"));
            Claim a=e.claim("w",1,100);
            e.complete(e.completeAndClaim(a,1,null,2,"w",100).next(),2,null,3);
            afterPatches=Json.write(e.get("legacy-id"));
            assertEquals("SUCCEEDED",e.get("legacy-id").get("status"));
        }
        try(JournalEngine e=new JournalEngine(dir)){assertEquals(afterPatches,Json.write(e.get("legacy-id")),"mixed full + patch WAL");}
    }
    @Test void diffRoundTripsAndRefusesRemovals() {
        Map<String,Object> before=Workflow.newRun(JournalEngineTest.def("[{\"id\":\"a\",\"compensate\":\"echo\"},{\"id\":\"b\",\"maxAttempts\":1,\"dependsOn\":[\"a\"]}]"),"k","x",0);
        Map<String,Object> after=Workflow.mutableCopy(before);
        Claim c=Workflow.claim(after,"a","w",1,10).claim();
        Workflow.complete(after,c,Map.of("v",1),null,2,1);
        Map<String,Object> patch=Patch.diff(before,after);
        assertFalse(patch.containsKey("added"));
        assertFalse(Json.write(patch).contains("\"spec\""),"unchanged specs are not rewritten");
        assertEquals(Json.write(after),Json.write(Patch.apply(before,patch)));
        Map<String,Object> removed=Workflow.mutableCopy(after);removed.remove("signals");
        assertNull(Patch.diff(after,removed),"unexpressible change falls back to a full record");
    }
}
