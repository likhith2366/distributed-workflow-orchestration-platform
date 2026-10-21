package dev.conductor;

import dev.conductor.aws.*;
import dev.conductor.core.*;
import dev.conductor.journal.*;
import dev.conductor.worker.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

/**
 * The storage backend and worker runtime selected by configuration, closed in dependency order.
 *
 * <table>
 *   <tr><th>store</th><th>coordinator role</th><th>worker role</th></tr>
 *   <tr><td>journal</td><td>WAL engine + embedded workers</td><td>HTTP remote worker</td></tr>
 *   <tr><td>dynamodb</td><td>stateless API + outbox relay (+ optional SQS workers)</td><td>SQS workers</td></tr>
 * </table>
 */
public final class Backend implements AutoCloseable {
    private final WorkflowEngine engine;
    private final EffectLedger ledger;
    private final AutoCloseable workers;
    private final Supplier<Map<String,Object>> workerStats;
    private final OutboxRelay relay;
    private final AwsClients aws;

    private Backend(WorkflowEngine engine,EffectLedger ledger,AutoCloseable workers,Supplier<Map<String,Object>> workerStats,
                    OutboxRelay relay,AwsClients aws) {
        this.engine=engine;this.ledger=ledger;this.workers=workers;this.workerStats=workerStats;this.relay=relay;this.aws=aws;
    }
    public static Backend create(ConductorProperties p)throws IOException {
        if(p.dynamo())return dynamo(p);
        if(p.worker()) {
            RemoteWorker remote=new RemoteWorker(p.coordinatorUrl(),p.apiToken(),Math.max(1,p.workers()));
            return new Backend(null,null,remote,()->Map.of("mode","remote"),null,null);
        }
        PartitionedJournal.Store store=PartitionedJournal.open(Path.of(p.dataDir()),p.journalShards());
        EmbeddedWorkers embedded=new EmbeddedWorkers(store.engine(),store.ledger(),p.workers(),p.leaseMs());
        return new Backend(store.engine(),store.ledger(),embedded,embedded::stats,null,null);
    }
    private static Backend dynamo(ConductorProperties p) {
        AwsSettings settings=p.awsSettings();
        AwsClients aws=new AwsClients(settings);
        try {
            String queueUrl=settings.createResources()?AwsResources.ensure(aws,settings)
                    :aws.sqs.getQueueUrl(b->b.queueName(settings.queue())).queueUrl();
            DynamoEngine engine=new DynamoEngine(aws,settings,queueUrl);
            DynamoLedger ledger=new DynamoLedger(engine,aws);
            int count=p.worker()?Math.max(1,p.workers()):p.workers();
            SqsWorkers sqs=count==0?null:new SqsWorkers(engine,ledger,aws.sqs,queueUrl,count,p.leaseMs());
            OutboxRelay relay=p.worker()?null:new OutboxRelay(engine,p.relayIntervalMs(),2_000);
            return new Backend(engine,ledger,sqs,sqs==null?()->Map.of("mode","none"):sqs::stats,relay,aws);
        } catch(RuntimeException e){aws.close();throw e;}
    }
    public WorkflowEngine engine() {
        if(engine==null)throw new IllegalStateException("This process has no local engine");
        return engine;
    }
    public EffectLedger ledger(){return ledger;}
    public Map<String,Object> workerStats(){return workerStats.get();}
    /** Long-poll support for remote journal workers. */
    public Optional<LocalEngine> journal(){return engine instanceof LocalEngine j?Optional.of(j):Optional.empty();}
    @Override public void close() {
        for(AutoCloseable c:new AutoCloseable[]{workers,relay,ledger,engine,aws}) {
            if(c==null)continue;
            try{c.close();}catch(Exception e){System.err.println("Shutdown: "+e);}
        }
    }
}
