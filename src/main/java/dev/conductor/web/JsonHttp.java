package dev.conductor.web;

import dev.conductor.core.Json;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.springframework.http.*;

/**
 * Request and response bodies use the engine's strict codec (duplicate keys and non-finite
 * numbers are rejected), so what the API accepts is exactly what the journal can persist.
 */
final class JsonHttp {
    private JsonHttp() {}
    static final int MAX_BODY=256*1024;
    static ResponseEntity<String> json(HttpStatusCode status,Object body) {
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(Json.write(body));
    }
    static ResponseEntity<String> ok(Object body){return json(HttpStatus.OK,body);}
    static Map<String,Object> body(HttpServletRequest request)throws IOException {
        byte[] bytes=request.getInputStream().readNBytes(MAX_BODY+1);
        if(bytes.length>MAX_BODY)throw new IllegalArgumentException("Payload exceeds 256 KiB");
        String text=new String(bytes,StandardCharsets.UTF_8);
        return text.isBlank()?new LinkedHashMap<>():Json.obj(Json.parse(text));
    }
}
