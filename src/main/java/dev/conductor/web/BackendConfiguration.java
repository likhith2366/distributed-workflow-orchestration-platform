package dev.conductor.web;

import dev.conductor.*;
import java.io.IOException;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.*;

@Configuration(proxyBeanMethods=false)
public class BackendConfiguration {
    @Bean(destroyMethod="close")
    Backend backend(ConductorProperties properties,org.springframework.core.env.Environment env)throws IOException {
        if(!properties.worker()) {
            String bind=env.getProperty("server.address","127.0.0.1");
            boolean loopback=bind.equals("127.0.0.1")||bind.equals("localhost")||bind.equals("::1");
            if(!loopback&&properties.apiToken().length()<24)
                throw new IllegalArgumentException("Non-loopback binding requires API_TOKEN of at least 24 characters");
        }
        return Backend.create(properties);
    }
    @Bean
    HealthIndicator engineHealth(Backend backend,ConductorProperties properties) {
        return ()->{
            if(properties.worker()&&!properties.dynamo())return Health.up().withDetail("role","remote-worker").build();
            return backend.engine().healthy()?Health.up().withDetail("backend",backend.engine().backend()).build()
                    :Health.down().withDetail("reason","durability failure; restart required").build();
        };
    }
}
