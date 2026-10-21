package dev.conductor;

import dev.conductor.aws.AwsSettings;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Bound from {@code application.yaml}, which maps the documented environment variables. */
@ConfigurationProperties("conductor")
public record ConductorProperties(String store,String role,String dataDir,int workers,long leaseMs,String apiToken,
                                  String coordinatorUrl,int maxConcurrentRequests,long relayIntervalMs,int journalShards,Aws aws) {
    public record Aws(String region,String endpoint,String table,String queue,String bucket,String eventBus,
                      boolean createResources,int payloadThresholdBytes) {}
    public ConductorProperties {
        store=store==null?"journal":store.toLowerCase();
        role=role==null?"coordinator":role.toLowerCase();
        apiToken=apiToken==null?"":apiToken;
        if(!store.equals("journal")&&!store.equals("dynamodb"))throw new IllegalArgumentException("conductor.store must be journal or dynamodb");
        if(!role.equals("coordinator")&&!role.equals("worker"))throw new IllegalArgumentException("conductor.role must be coordinator or worker");
        if(workers<0||workers>256)throw new IllegalArgumentException("WORKERS must be 0..256");
        if(leaseMs<1000||leaseMs>600_000)throw new IllegalArgumentException("lease must be 1..600 seconds");
        if(journalShards<1||journalShards>64)throw new IllegalArgumentException("JOURNAL_SHARDS must be 1..64");
    }
    public boolean dynamo(){return store.equals("dynamodb");}
    public boolean worker(){return role.equals("worker");}
    public AwsSettings awsSettings() {
        return new AwsSettings(aws.region(),aws.endpoint(),aws.table(),aws.queue(),aws.bucket(),aws.eventBus(),
                aws.createResources(),aws.payloadThresholdBytes());
    }
}
