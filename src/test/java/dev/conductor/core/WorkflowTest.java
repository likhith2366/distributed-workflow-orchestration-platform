package dev.conductor.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class WorkflowTest {
    static Map<String,Object> def(String steps) {
        return Json.obj(Json.parse("{\"name\":\"test\",\"version\":\"1\",\"steps\":"+steps+"}"));
    }
    @Test void strictJsonAndRoundTrip() {
        String s="{\"a\":[null,true,4,\"x\\n\\u0041\"]}";
        assertEquals(Json.parse(s),Json.parse(Json.write(Json.parse(s))));
        assertThrows(IllegalArgumentException.class,()->Json.parse("{\"x\":1,\"x\":2}"));
        assertThrows(IllegalArgumentException.class,()->Json.parse("[01]"));
        assertThrows(IllegalArgumentException.class,()->Json.parse("1e999"));
    }
    @Test void deepCopyIsolatesNestedValues() {
        Map<String,Object> original=Json.obj(Json.parse("{\"a\":[{\"b\":\"x\\n\\t\\u0001\"}],\"n\":1.5}"));
        Map<String,Object> copy=Json.copy(original);
        assertEquals(Json.write(original),Json.write(copy));
        Json.obj(Json.arr(copy.get("a")).get(0)).put("b","changed");
        assertNotEquals(Json.write(original),Json.write(copy));
        assertThrows(IllegalArgumentException.class,()->Json.deepCopy(Double.NaN));
    }
    @Test void dagAndActionValidation() {
        assertThrows(IllegalArgumentException.class,()->Workflow.validate(def("[{\"id\":\"a\",\"dependsOn\":[\"b\"]},{\"id\":\"b\",\"dependsOn\":[\"a\"]}]")));
        assertThrows(IllegalArgumentException.class,()->Workflow.validate(def("[{\"id\":\"a\",\"dependsOn\":[\"missing\"]}]")));
        assertThrows(IllegalArgumentException.class,()->Workflow.validate(def("[{\"id\":\"a\",\"action\":\"shell\"}]")));
        assertThrows(IllegalArgumentException.class,()->Workflow.validate(def("[{\"id\":\"a\",\"maxAttempts\":0}]")));
        assertThrows(IllegalArgumentException.class,()->Workflow.validate(def("[{\"id\":\"$undo:a\"}]")),"compensation namespace reserved");
    }
    @Test void newRunActivatesRootsAndDelays() {
        Map<String,Object> run=Workflow.newRun(def("[{\"id\":\"a\",\"delayMs\":50},{\"id\":\"b\",\"action\":\"approval\"},{\"id\":\"c\",\"dependsOn\":[\"a\"]}]"),"k","id",10);
        Map<String,Object> tasks=Workflow.tasks(run);
        assertEquals("READY",Workflow.status(Json.obj(tasks.get("a"))));
        assertEquals(60L,Json.obj(tasks.get("a")).get("readyAt"));
        assertEquals(60L,Workflow.dueAt(Json.obj(tasks.get("a"))));
        assertEquals("WAITING",Workflow.status(Json.obj(tasks.get("b"))));
        assertEquals(Long.MAX_VALUE,Workflow.dueAt(Json.obj(tasks.get("c"))));
    }
    @Test void transitionsOnAMutableCopyNeverChangeTheOriginal() {
        // Copy-on-write commits share nested values; every transition must leave the source intact.
        Map<String,Object> run=Workflow.newRun(def("[{\"id\":\"a\",\"compensate\":\"ledger\"},{\"id\":\"b\",\"maxAttempts\":1,\"dependsOn\":[\"a\"]},{\"id\":\"w\",\"action\":\"approval\"}]"),"k","id",0);
        long now=0;
        for(String step:List.of("claim-a","complete-a","claim-b","fail-b","claim-undo","complete-undo")) {
            String before=Json.write(run);
            Map<String,Object> next=Workflow.mutableCopy(run);
            switch(step) {
                case "claim-a" -> Workflow.claim(next,"a","w",++now,100);
                case "complete-a" -> Workflow.complete(next,claimOf(next,"a"),Map.of("r",List.of(1)),null,++now,1);
                case "claim-b" -> Workflow.claim(next,"b","w",++now,100);
                case "fail-b" -> Workflow.complete(next,claimOf(next,"b"),null,"boom",++now,2);
                case "claim-undo" -> Workflow.claim(next,"$undo:a","w",++now,100);
                default -> Workflow.complete(next,claimOf(next,"$undo:a"),1,null,++now,3);
            }
            assertEquals(before,Json.write(run),"source mutated by "+step);
            run=next;
        }
        assertEquals("COMPENSATED",run.get("status"));
    }
    private static Claim claimOf(Map<String,Object> run,String task) {
        Map<String,Object> t=Json.obj(Workflow.tasks(run).get(task));
        return new Claim("id",task,"w",Json.num(t,"fence",0),Json.num(t,"attempt",0),"id/"+task,Json.obj(t.get("spec")),0);
    }
    @Test void retryBackoffDoublesAndCaps() {
        Map<String,Object> run=Workflow.newRun(def("[{\"id\":\"a\",\"backoffMs\":100,\"maxAttempts\":5}]"),"k","id",0);
        Claim c=Workflow.claim(run,"a","w",0,1000).claim();
        Workflow.complete(run,c,null,"boom",10,1);
        assertEquals(110L,Json.obj(Workflow.tasks(run).get("a")).get("readyAt"));
        c=Workflow.claim(run,"a","w",110,1000).claim();
        Workflow.complete(run,c,null,"boom",120,2);
        assertEquals(320L,Json.obj(Workflow.tasks(run).get("a")).get("readyAt"),"second retry waits 2x base");
    }
    @Test void nullErrorMessageStillCountsAsFailure()throws Exception {
        Claim c=new Claim("e","t","w",1,1,"e/t",Map.of("action","echo"),0);
        assertEquals(Map.of("ok",true),Activities.execute(c,null));
        Claim fail=new Claim("e","t","w",1,1,"e/t",Map.of("action","fail"),0);
        assertThrows(IllegalStateException.class,()->Activities.execute(fail,null));
    }
    @Test void warnsOnlyOnJdkBuildsWithTheVirtualThreadTimerBug() {
        assertTrue(dev.conductor.ConductorApplicationAccess.warn("25.0.2+10"));
        assertFalse(dev.conductor.ConductorApplicationAccess.warn("25.0.4.1+1"));
        assertFalse(dev.conductor.ConductorApplicationAccess.warn("21.0.2+13"));
        assertFalse(dev.conductor.ConductorApplicationAccess.warn("26+35"));
    }
    @Test void latencyRecorderUsesRecentWindow() {
        LatencyRecorder r=new LatencyRecorder(4);
        for(long v:new long[]{1000,1000,1,2,3,4})r.record(v);
        Map<String,Object> s=r.snapshot("d");
        assertEquals(4,s.get("dSampleCount"));assertEquals(6L,s.get("dTotalCount"));assertEquals(4L,s.get("dP99Ms"));
    }
}
