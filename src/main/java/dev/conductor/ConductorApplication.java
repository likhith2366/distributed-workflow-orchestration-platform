package dev.conductor;

import java.util.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * One artifact, two roles. {@code CONDUCTOR_ROLE=coordinator} (default) serves the API and console;
 * {@code CONDUCTOR_ROLE=worker} runs only task workers, with no HTTP listener.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ConductorApplication {
    public static void main(String[] args) {
        warnOnAffectedJdk(Runtime.version());
        SpringApplication app=new SpringApplication(ConductorApplication.class);
        if(isWorker(args)) {
            app.setWebApplicationType(WebApplicationType.NONE);
            // Worker loops run on virtual (daemon) threads; keep the JVM alive until shutdown.
            app.setDefaultProperties(Map.of("spring.main.keep-alive","true"));
        }
        app.run(args);
    }
    /**
     * JDK-8370887: on early JDK 25 updates, cancelling timed virtual-thread parks can corrupt the
     * scheduler's timer heap, so short sleeps and timeouts stall until a long timer fires. Long
     * polls and idle-worker waits cancel timed parks constantly; measured on 25.0.2, 10 ms
     * activities stalled for ~10 s. Verified fixed in Temurin 25.0.4.1; JDK 21 is unaffected.
     */
    static boolean warnOnAffectedJdk(Runtime.Version v) {
        boolean affected=v.feature()==25&&v.update()<4;
        if(affected)System.err.println("WARNING: Java "+v+" has JDK-8370887 (virtual-thread timers can stall for seconds). "
                +"Use JDK 21 or JDK 25.0.4+; see docs/scaling.md.");
        return affected;
    }
    static boolean isWorker(String[] args) {
        for(String arg:args)if(arg.equals("--conductor.role=worker"))return true;
        String role=Objects.requireNonNullElse(System.getenv("CONDUCTOR_ROLE"),System.getProperty("conductor.role",""));
        return role.equalsIgnoreCase("worker");
    }
}
