package dev.conductor.web;

import dev.conductor.*;
import dev.conductor.core.*;
import dev.conductor.journal.LocalEngine;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** Trusted worker protocol for remote journal workers. The coordinator clock decides lease validity. */
@RestController
@RequestMapping("/api/worker")
class WorkerController {
    private static final long MAX_WAIT_MS=20_000;
    private final Backend backend;
    private final long leaseMs;
    WorkerController(Backend backend,ConductorProperties properties){this.backend=backend;this.leaseMs=properties.leaseMs();}

    /**
     * Long-polls for due work; an empty object means none. The wait is capped at a third of the
     * lease: a poll left behind by a crashed worker ends before that worker's lease can expire,
     * so it cannot claim the reclaimable task on behalf of a dead process.
     */
    @PostMapping("/claim")
    ResponseEntity<String> claim(HttpServletRequest request)throws IOException,InterruptedException {
        Map<String,Object> body=JsonHttp.body(request);
        String owner=Json.str(body,"owner","");
        long waitMs=Math.min(Math.min(MAX_WAIT_MS,leaseMs/3),Math.max(0,Json.num(body,"waitMs",0)));
        WorkflowEngine engine=backend.engine();
        long deadline=System.currentTimeMillis()+waitMs;
        while(true) {
            LocalEngine journal=backend.journal().orElse(null);
            long generation=journal==null?0:journal.workGeneration();
            Claim claim=engine.claim(owner,System.currentTimeMillis(),leaseMs);
            if(claim!=null)return JsonHttp.ok(claim.toWire());
            long remaining=deadline-System.currentTimeMillis();
            if(remaining<=0||journal==null)return JsonHttp.ok(Map.of());
            journal.awaitWork(generation,remaining);
        }
    }
    @PostMapping("/heartbeat")
    ResponseEntity<String> heartbeat(HttpServletRequest request)throws IOException {
        Claim claim=Claim.fromWire(Json.obj(JsonHttp.body(request).get("claim")));
        return JsonHttp.ok(Map.of("accepted",backend.engine().heartbeat(claim,System.currentTimeMillis(),leaseMs)));
    }
    @PostMapping("/complete")
    ResponseEntity<String> complete(HttpServletRequest request)throws IOException {
        Map<String,Object> body=JsonHttp.body(request);
        Claim claim=Claim.fromWire(Json.obj(body.get("claim")));
        // With "next": true the coordinator also claims the worker's next task in the same commit.
        if(Boolean.TRUE.equals(body.get("next"))&&backend.journal().isPresent()) {
            LocalEngine.Handoff h=backend.journal().get().completeAndClaim(claim,body.get("result"),Json.str(body,"error",null),
                    System.currentTimeMillis(),claim.owner(),leaseMs);
            Map<String,Object> response=new LinkedHashMap<>();
            response.put("accepted",h.accepted());response.put("next",h.next()==null?Map.of():h.next().toWire());
            return JsonHttp.ok(response);
        }
        boolean accepted=backend.engine().complete(claim,body.get("result"),Json.str(body,"error",null),System.currentTimeMillis());
        return JsonHttp.ok(Map.of("accepted",accepted));
    }
    @PostMapping("/effect")
    ResponseEntity<String> effect(HttpServletRequest request)throws IOException {
        Map<String,Object> body=JsonHttp.body(request);
        Claim claim=Claim.fromWire(Json.obj(body.get("claim")));
        Map<String,Object> response=new LinkedHashMap<>();
        response.put("result",backend.ledger().apply(claim.idempotencyKey(),body.get("value")));
        return JsonHttp.ok(response);
    }
}
