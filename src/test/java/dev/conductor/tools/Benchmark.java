package dev.conductor.tools;

import dev.conductor.core.*;
import dev.conductor.journal.*;
import dev.conductor.worker.EmbeddedWorkers;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Closed-loop benchmark of the journal engine with embedded workers. Each of {@code clients}
 * concurrent clients submits a three-step workflow (ledger, ledger, echo), waits for it to finish,
 * and repeats. Concurrent submissions share group commits, as real API callers do. No HTTP.
 *
 * <pre>Benchmark [--workflows N] [--clients C] [--workers W] [--activity-ms A] [--out file.json]</pre>
 *
 * {@code --activity-ms} adds simulated downstream latency to every step, which is what makes a
 * worker pool's size matter. Steady-state throughput is measured between the 10th and 90th
 * percentile completion times, excluding ramp-up and drain.
 */
public final class Benchmark {
    public static void main(String[] args)throws Exception {
        Map<String,String> o=options(args);
        int count=Integer.parseInt(o.getOrDefault("workflows","1000"));
        int clients=Integer.parseInt(o.getOrDefault("clients","32"));
        int workerCount=Integer.parseInt(o.getOrDefault("workers","8"));
        long activityMs=Long.parseLong(o.getOrDefault("activity-ms","0"));
        int shards=Integer.parseInt(o.getOrDefault("shards",Integer.toString(Integer.getInteger("conductor.journalShards",1))));
        if(count<10||clients<1)throw new IllegalArgumentException("need >= 10 workflows and >= 1 client");
        Path dir=Files.createTempDirectory("conductor-benchmark-");
        String latency=activityMs>0?",\"latencyMs\":"+activityMs:"";
        Map<String,Object> definition=Json.obj(Json.parse("{\"name\":\"benchmark\",\"version\":\"1\",\"steps\":["
                +"{\"id\":\"reserve\",\"action\":\"ledger\",\"maxAttempts\":20"+latency+"},"
                +"{\"id\":\"charge\",\"action\":\"ledger\",\"maxAttempts\":20,\"dependsOn\":[\"reserve\"]"+latency+"},"
                +"{\"id\":\"notify\",\"action\":\"echo\",\"maxAttempts\":20,\"dependsOn\":[\"charge\"]"+latency+"}]}"));
        long[] latencies=new long[count],finished=new long[count];
        AtomicInteger next=new AtomicInteger();
        var os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        long cpuBegin=os.getProcessCpuTime();
        long begin=System.nanoTime();
        Map<String,Object> workerMetrics,engineMetrics,ledgerMetrics;
        try(PartitionedJournal.Store store=PartitionedJournal.open(dir,shards);
            EmbeddedWorkers workers=new EmbeddedWorkers(store.engine(),store.ledger(),workerCount,3000);
            ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            LocalEngine engine=store.engine();EffectLedger ledger=store.ledger();
            List<Future<?>> futures=new ArrayList<>();
            for(int c=0;c<clients;c++)futures.add(pool.submit(()->{
                for(int i;(i=next.getAndIncrement())<count;) {
                    long start=System.nanoTime();
                    String id=engine.start(definition,"bench-"+i,System.currentTimeMillis());
                    String status;
                    // Event-driven: woken by the commit that makes the run terminal, not by polling.
                    while(!Workflow.terminal(status=Json.str(engine.awaitTerminal(id,10_000),"status",""))) {
                        if(System.nanoTime()-begin>600_000_000_000L)throw new IllegalStateException("Benchmark timed out");
                    }
                    if(!status.equals("SUCCEEDED"))throw new IllegalStateException("Benchmark execution failed: "+status);
                    long end=System.nanoTime();
                    latencies[i]=(end-start)/1_000;finished[i]=end;
                }
                return null;
            }));
            for(Future<?> f:futures)f.get();
            workerMetrics=workers.stats();engineMetrics=engine.stats();
            ledgerMetrics=ledger instanceof JournalLedger l?l.stats():((PartitionedJournal.Ledger)ledger).stats();
            if(ledger.size()!=count*2L)throw new AssertionError("Effect count mismatch");
        }
        double seconds=(System.nanoTime()-begin)/1e9;
        double cpuSeconds=(os.getProcessCpuTime()-cpuBegin)/1e9;
        Arrays.sort(latencies);Arrays.sort(finished);
        int lo=count/10,hi=count*9/10;
        double steady=(hi-lo)/((finished[hi]-finished[lo])/1e9);
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("timestamp",Instant.now().toString());result.put("scope","local in-process durable engine; no network; not AWS");
        result.put("harness","closed loop, "+clients+" concurrent clients, submit then awaitTerminal (commit-signalled, no polling)");
        result.put("java",System.getProperty("java.version"));result.put("os",System.getProperty("os.name"));
        result.put("availableProcessors",Runtime.getRuntime().availableProcessors());result.put("maxHeapBytes",Runtime.getRuntime().maxMemory());
        result.put("workflows",count);result.put("stepsPerWorkflow",3);result.put("activityMs",activityMs);result.put("ledgerEffects",count*2);
        result.put("workers",workerCount);result.put("clients",clients);result.put("journalShards",shards);result.put("seconds",seconds);
        result.put("workflowsPerSecond",count/seconds);result.put("steadyWorkflowsPerSecond",steady);
        // Recorded in microseconds: unloaded latency is a handful of ~1 ms fsyncs.
        result.put("workflowP50Ms",LatencyRecorder.percentile(latencies,.50)/1000);
        result.put("workflowP99Ms",LatencyRecorder.percentile(latencies,.99)/1000);
        result.put("workflowP50Micros",LatencyRecorder.percentile(latencies,.50));
        result.put("workflowP99Micros",LatencyRecorder.percentile(latencies,.99));
        result.put("processCpuSeconds",cpuSeconds);result.put("processCpuCores",cpuSeconds/seconds);
        result.put("workersMetrics",workerMetrics);result.put("engineMetrics",engineMetrics);result.put("ledgerMetrics",ledgerMetrics);
        result.put("batchSizeLimit",Integer.getInteger("conductor.batchSize",64));

        result.put("durability","Every transition and effect covered by FileChannel.force(true) before acknowledgment; concurrent writes share a group commit");
        Path out=Path.of(o.getOrDefault("out","benchmarks/latest.json"));
        Files.createDirectories(out.toAbsolutePath().getParent());Files.writeString(out,Json.write(result));
        System.out.println(Json.write(result));
        deleteQuietly(dir);
    }
    static Map<String,String> options(String[] args) {
        Map<String,String> o=new HashMap<>();
        for(int i=0;i<args.length;i++) {
            if(!args[i].startsWith("--")||i+1>=args.length)throw new IllegalArgumentException("Expected --name value pairs");
            o.put(args[i].substring(2),args[++i]);
        }
        return o;
    }
    static void deleteQuietly(Path dir) {
        try(var walk=Files.walk(dir)){walk.sorted(Comparator.reverseOrder()).forEach(p->{try{Files.delete(p);}catch(Exception ignored){}});}
        catch(Exception ignored){}
    }
}
