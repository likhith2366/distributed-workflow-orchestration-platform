package dev.conductor.web;

import dev.conductor.Backend;
import java.io.IOException;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
class StatusController {
    private final Backend backend;
    StatusController(Backend backend){this.backend=backend;}

    /** Liveness plus durability: 503 once the journal writer is poisoned and a restart is required. */
    @GetMapping("/healthz")
    ResponseEntity<String> health() {
        boolean healthy=backend.engine().healthy();
        return JsonHttp.json(healthy?HttpStatus.OK:HttpStatus.SERVICE_UNAVAILABLE,
                Map.of("status",healthy?"ok":"durability-failure","backend",backend.engine().backend()));
    }
    @GetMapping("/api/metrics")
    ResponseEntity<String> metrics()throws IOException {
        Map<String,Object> body=new LinkedHashMap<>();
        body.put("engine",backend.engine().stats());body.put("workers",backend.workerStats());
        body.put("ledgerEffects",backend.ledger().size());
        return JsonHttp.ok(body);
    }
}
