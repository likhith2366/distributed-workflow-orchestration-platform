package dev.conductor.web;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import dev.conductor.core.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@SpringBootTest
@AutoConfigureMockMvc
class ApiTest {
    static final String TOKEN="test-token-with-at-least-24-chars";
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry)throws Exception {
        Path dir=Files.createTempDirectory("conductor-api-");
        registry.add("conductor.data-dir",dir::toString);
        registry.add("conductor.api-token",()->TOKEN);
        registry.add("conductor.workers",()->"4");
    }
    @Autowired MockMvc mvc;

    private MockHttpServletRequestBuilder auth(MockHttpServletRequestBuilder b){return b.header("Authorization","Bearer "+TOKEN);}
    private Map<String,Object> json(MockHttpServletRequestBuilder b)throws Exception {
        String body=mvc.perform(auth(b)).andExpect(status().is2xxSuccessful()).andReturn().getResponse().getContentAsString();
        return Json.obj(Json.parse(body));
    }
    private Map<String,Object> await(String id,String field,String expected)throws Exception {
        long deadline=System.currentTimeMillis()+10_000;
        while(System.currentTimeMillis()<deadline) {
            Map<String,Object> run=json(get("/api/executions/"+id));
            if(expected.equals(field.isEmpty()?run.get("status"):Json.obj(Json.obj(run.get("tasks")).get(field)).get("status")))return run;
            Thread.sleep(20);
        }
        fail("Timed out waiting for "+field+"="+expected);return null;
    }

    @Test void guardsRejectUnauthenticatedAndCrossOriginRequests()throws Exception {
        mvc.perform(get("/api/metrics")).andExpect(status().isUnauthorized());
        mvc.perform(auth(post("/api/executions")).header("Origin","https://untrusted.example").content("{}"))
                .andExpect(status().isForbidden());
        mvc.perform(get("/healthz")).andExpect(status().isOk())
                .andExpect(header().string("X-Content-Type-Options","nosniff"))
                .andExpect(header().exists("Content-Security-Policy"));
    }
    @Test void validationErrorsAreClientErrors()throws Exception {
        mvc.perform(auth(post("/api/executions")).header("Idempotency-Key","bad").content("{\"name\":\"bad\",\"version\":\"1\",\"steps\":[]}"))
                .andExpect(status().isBadRequest());
        mvc.perform(auth(post("/api/executions")).content("{\"name\":\"x\",\"version\":\"1\",\"steps\":[{\"id\":\"a\"}]}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("Idempotency-Key required (1..128 chars)"));
        mvc.perform(auth(post("/api/executions")).header("Idempotency-Key","dup").content("{\"a\":1,\"a\":2}"))
                .andExpect(status().isBadRequest());
        mvc.perform(auth(post("/api/executions")).header("Idempotency-Key","big").content("{\"x\":\""+"a".repeat(300_000)+"\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(auth(get("/api/executions/missing"))).andExpect(status().isNotFound());
        mvc.perform(auth(delete("/api/executions"))).andExpect(status().isMethodNotAllowed());
    }
    @Test void approvalWorkflowRunsThroughEmbeddedWorkers()throws Exception {
        String definition="{\"name\":\"api\",\"version\":\"1\",\"steps\":[{\"id\":\"pay\",\"action\":\"ledger\",\"failUntil\":1,\"backoffMs\":10},"
                +"{\"id\":\"review\",\"action\":\"approval\",\"dependsOn\":[\"pay\"]},{\"id\":\"ship\",\"dependsOn\":[\"review\"]}]}";
        String id=Json.str(json(post("/api/executions").header("Idempotency-Key","api-flow").content(definition)),"id","");
        assertEquals(id,Json.str(json(post("/api/executions").header("Idempotency-Key","api-flow").content(definition)),"id",""),"idempotent");
        await(id,"review","WAITING");
        json(post("/api/executions/"+id+"/signal").content("{\"step\":\"review\"}"));
        Map<String,Object> run=await(id,"","SUCCEEDED");
        assertEquals(2L,Json.obj(Json.obj(run.get("tasks")).get("pay")).get("attempt"),"retried once");
        Map<String,Object> metrics=json(get("/api/metrics"));
        assertEquals("journal",Json.obj(metrics.get("engine")).get("backend"));
        assertTrue(((Number)metrics.get("ledgerEffects")).longValue()>=1);
        String list=mvc.perform(auth(get("/api/executions?limit=500"))).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertTrue(Json.arr(Json.parse(list)).stream().anyMatch(r->id.equals(Json.obj(r).get("id"))));
        mvc.perform(auth(get("/api/executions?limit=0"))).andExpect(status().isBadRequest());
    }
    @Test void cancelStopsWaitingExecution()throws Exception {
        String id=Json.str(json(post("/api/executions").header("Idempotency-Key","api-cancel")
                .content("{\"name\":\"c\",\"version\":\"1\",\"steps\":[{\"id\":\"a\",\"action\":\"approval\"}]}")),"id","");
        assertEquals("CANCELLED",json(post("/api/executions/"+id+"/cancel")).get("status"));
        mvc.perform(auth(post("/api/executions/"+id+"/signal")).content("{\"step\":\"a\"}")).andExpect(status().isBadRequest());
    }
    @Test void workerProtocolLongPollReturnsEmptyWhenIdle()throws Exception {
        long start=System.currentTimeMillis();
        Map<String,Object> claim=json(post("/api/worker/claim").content("{\"owner\":\"probe\",\"waitMs\":200}"));
        assertTrue(claim.isEmpty()||claim.containsKey("fence"));
        assertTrue(System.currentTimeMillis()-start<5_000);
    }
}
