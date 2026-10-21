package dev.conductor.web;

import dev.conductor.ConductorProperties;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.Semaphore;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Security headers everywhere; for {@code /api/**}: same-origin check, constant-time bearer token
 * comparison, and a concurrency limit that sheds load with 429 instead of queueing unboundedly.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApiGuardFilter extends OncePerRequestFilter {
    private static final String CSP="default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            +"connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";
    private final byte[] expected;
    private final Semaphore permits;
    public ApiGuardFilter(ConductorProperties properties) {
        expected=properties.apiToken().isEmpty()?null:("Bearer "+properties.apiToken()).getBytes(StandardCharsets.UTF_8);
        permits=new Semaphore(properties.maxConcurrentRequests());
    }
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)
            throws ServletException,IOException {
        response.setHeader("X-Content-Type-Options","nosniff");
        response.setHeader("Referrer-Policy","no-referrer");
        response.setHeader("Content-Security-Policy",CSP);
        String path=request.getRequestURI();
        if(!path.startsWith("/api/")) {
            // Hashed asset names are immutable; everything else must revalidate.
            response.setHeader("Cache-Control",path.startsWith("/assets/")?"public, max-age=31536000, immutable":"no-store");
            chain.doFilter(request,response);return;
        }
        response.setHeader("Cache-Control","no-store");
        if(!sameOrigin(request.getHeader("Origin"),request.getHeader("Host"))){reject(response,403,"Cross-origin requests are disabled");return;}
        if(expected!=null&&!MessageDigest.isEqual(expected,
                Objects.toString(request.getHeader("Authorization"),"").getBytes(StandardCharsets.UTF_8))) {
            reject(response,401,"Bearer token required");return;
        }
        if(!permits.tryAcquire()){reject(response,429,"Busy");return;}
        try{chain.doFilter(request,response);}
        finally{permits.release();}
    }
    static boolean sameOrigin(String origin,String host) {
        if(origin==null)return true;
        try{return host!=null&&host.equalsIgnoreCase(URI.create(origin).getRawAuthority());}
        catch(IllegalArgumentException e){return false;}
    }
    private static void reject(HttpServletResponse response,int status,String message)throws IOException {
        response.setStatus(status);response.setContentType("application/json");
        response.getOutputStream().write(dev.conductor.core.Json.write(Map.of("error",message)).getBytes(StandardCharsets.UTF_8));
    }
}
