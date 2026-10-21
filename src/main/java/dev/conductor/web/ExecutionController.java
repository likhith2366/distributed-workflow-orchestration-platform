package dev.conductor.web;

import dev.conductor.Backend;
import dev.conductor.core.*;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/executions")
class ExecutionController {
    private final Backend backend;
    ExecutionController(Backend backend){this.backend=backend;}

    @PostMapping
    ResponseEntity<String> start(HttpServletRequest request,@RequestHeader(name="Idempotency-Key",required=false) String key)throws IOException {
        String id=backend.engine().start(JsonHttp.body(request),key,System.currentTimeMillis());
        return JsonHttp.json(HttpStatus.ACCEPTED,Map.of("id",id));
    }
    @GetMapping
    ResponseEntity<String> list(@RequestParam(defaultValue="100") int limit)throws IOException {
        if(limit<1||limit>500)throw new IllegalArgumentException("limit must be 1..500");
        return JsonHttp.ok(backend.engine().list(limit));
    }
    /** {@code waitMs} (max 30 s) long-polls until the execution is terminal, sparing clients a polling loop. */
    @GetMapping("/{id}")
    ResponseEntity<String> get(@PathVariable String id,@RequestParam(defaultValue="0") long waitMs)throws IOException,InterruptedException {
        if(waitMs<0||waitMs>30_000)throw new IllegalArgumentException("waitMs must be 0..30000");
        WorkflowEngine engine=backend.engine();
        return JsonHttp.ok(waitMs==0?engine.get(id):engine.awaitTerminal(id,waitMs));
    }
    @PostMapping("/{id}/signal")
    ResponseEntity<String> signal(@PathVariable String id,HttpServletRequest request)throws IOException {
        WorkflowEngine engine=backend.engine();
        engine.signal(id,Json.str(JsonHttp.body(request),"step",""),System.currentTimeMillis());
        return JsonHttp.ok(engine.get(id));
    }
    @PostMapping("/{id}/cancel")
    ResponseEntity<String> cancel(@PathVariable String id)throws IOException {
        WorkflowEngine engine=backend.engine();
        engine.cancel(id,System.currentTimeMillis());
        return JsonHttp.ok(engine.get(id));
    }
}
