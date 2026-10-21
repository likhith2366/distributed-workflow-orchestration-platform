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
import java.util.concurrent.locks.LockSupport;

/**
 * Closed-loop benchmark of the journal engine with embedded workers. Each of {@code clients}
 * concurrent clients submits a three-step workflow (ledger, ledger, echo), waits for it to finish,
 * and repeats. Concurrent submissions share group commits, as real API callers do. No HTTP.
 *
 * <pre>Benchmark [--workflows N] [--clients C] [--workers W] [--activity-ms A] [--rate R] [--out file.json]</pre>
 *
 * {@code --rate R} switches to open loop: workflows arrive at R per second regardless of how fast
 * earlier ones finish, paced by a spinning platform thread (Windows timers tick at ~15.6 ms, which
 * would release arrivals in bursts). Latency is measured from each workflow's intended arrival
 * time, so a stall cannot hide the queueing it causes (no coordinated omission).
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
        double rate=Double.parseDouble(o.getOrDefault("rate","0"));
        long[] latencies=new long[count],finished=new long[count],startLatencies=new long[count],startedAt=new long[count];
        GcPauses gc=GcPauses.install();
        Hiccups hiccups=Boolean.parseBoolean(o.getOrDefault("hiccup","false"))?Hiccups.start():null;
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
            if(rate>0) {
                // Open loop: one task per arrival, started on schedule whether or not earlier ones finished.
                long interval=(long)(1e9/rate);
                for(int i=0;i<count;i++) {
                    long intended=begin+i*interval;
                    while(true) {
                        long wait=intended-System.nanoTime();
                        if(wait<=0)break;
                        if(wait>3_000_000)LockSupport.parkNanos(wait-2_000_000);else Thread.onSpinWait();
                    }
                    int n=i;
                    futures.add(pool.submit(()->{
                        String id=engine.start(definition,"bench-"+n,System.currentTimeMillis());
                        startLatencies[n]=(System.nanoTime()-intended)/1_000;
                        String status;
                        while(!Workflow.terminal(status=Json.str(engine.awaitTerminal(id,10_000),"status",""))) {
                            if(System.nanoTime()-begin>600_000_000_000L)throw new IllegalStateException("Benchmark timed out");
                        }
                        if(!status.equals("SUCCEEDED"))throw new IllegalStateException("Benchmark execution failed: "+status);
                        long end=System.nanoTime();
                        latencies[n]=(end-intended)/1_000;finished[n]=end;startedAt[n]=intended;
                        return null;
                    }));
                }
            }
            else for(int c=0;c<clients;c++)futures.add(pool.submit(()->{
                for(int i;(i=next.getAndIncrement())<count;) {
                    long start=System.nanoTime();
                    String id=engine.start(definition,"bench-"+i,System.currentTimeMillis());
                    startLatencies[i]=(System.nanoTime()-start)/1_000;
                    String status;
                    // Event-driven: woken by the commit that makes the run terminal, not by polling.
                    while(!Workflow.terminal(status=Json.str(engine.awaitTerminal(id,10_000),"status",""))) {
                        if(System.nanoTime()-begin>600_000_000_000L)throw new IllegalStateException("Benchmark timed out");
                    }
                    if(!status.equals("SUCCEEDED"))throw new IllegalStateException("Benchmark execution failed: "+status);
                    long end=System.nanoTime();
                    latencies[i]=(end-start)/1_000;finished[i]=end;startedAt[i]=start;
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
        if(hiccups!=null)hiccups.stop();
        // Per-arrival latency in arrival order, before sorting: shows whether the tail clusters in time.
        long[] byArrival=latencies.clone(),endedAt=finished.clone();
        // Exclude a warm-up window (JIT, class loading) from latency percentiles; reported separately.
        int warm=Integer.parseInt(o.getOrDefault("warmup",Integer.toString(count/5)));
        long[] measured=Arrays.copyOfRange(latencies,Math.min(warm,count-1),count),warmup=Arrays.copyOfRange(latencies,0,Math.min(warm,count-1));
        Arrays.sort(measured);Arrays.sort(warmup);
        Arrays.sort(latencies);Arrays.sort(finished);Arrays.sort(startLatencies);
        int lo=count/10,hi=count*9/10;
        double steady=(hi-lo)/((finished[hi]-finished[lo])/1e9);
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("timestamp",Instant.now().toString());result.put("scope","local in-process durable engine; no network; not AWS");
        result.put("harness",rate>0?"open loop at "+rate+" workflows/s, latency from intended arrival time"
                :"closed loop, "+clients+" concurrent clients, submit then awaitTerminal (commit-signalled, no polling)");
        result.put("offeredRate",rate);
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
        result.put("workflowP999Micros",LatencyRecorder.percentile(latencies,.999));
        result.put("warmupArrivals",warm);
        // True completion rate over the measured window: completions / (last completion - first measured start).
        long firstMeasuredStart=Long.MAX_VALUE,lastEnd=0;
        for(int i=Math.min(warm,count-1);i<count;i++){firstMeasuredStart=Math.min(firstMeasuredStart,startedAt[i]);lastEnd=Math.max(lastEnd,endedAt[i]);}
        result.put("measuredRate",(count-Math.min(warm,count-1))/((lastEnd-firstMeasuredStart)/1e9));
        result.put("steadyP50Micros",LatencyRecorder.percentile(measured,.50));
        result.put("steadyP99Micros",LatencyRecorder.percentile(measured,.99));
        result.put("steadyP999Micros",LatencyRecorder.percentile(measured,.999));
        result.put("warmupP99Micros",LatencyRecorder.percentile(warmup,.99));
        // Worst arrivals by position (index, micros): where in the run the tail happened.
        List<long[]> worst=new ArrayList<>();
        for(int i=0;i<byArrival.length;i++)worst.add(new long[]{i,byArrival[i]});
        worst.sort((a,b)->Long.compare(b[1],a[1]));
        result.put("worstArrivals",worst.subList(0,Math.min(20,worst.size())).stream().map(x->List.of(x[0],x[1])).toList());
        result.put("workflowMaxMicros",latencies[latencies.length-1]);
        result.put("startP50Micros",LatencyRecorder.percentile(startLatencies,.50));
        result.put("startP99Micros",LatencyRecorder.percentile(startLatencies,.99));
        result.put("gc",gc.snapshot());
        result.put("tailAttribution",attribute(byArrival,startedAt,endedAt,Math.min(warm,count-1),gc,hiccups));
        result.put("processCpuSeconds",cpuSeconds);result.put("processCpuCores",cpuSeconds/seconds);
        result.put("workersMetrics",workerMetrics);result.put("engineMetrics",engineMetrics);result.put("ledgerMetrics",ledgerMetrics);
        result.put("batchSizeLimit",Integer.getInteger("conductor.batchSize",256));
        result.put("journalSync",dev.conductor.journal.Journal.WRITE_THROUGH?"write-through":"flush");

        result.put("durability","Every transition and effect durable before acknowledgment (write-through or write+force per journalSync); concurrent writes share a group commit");
        Path out=Path.of(o.getOrDefault("out","benchmarks/latest.json"));
        Files.createDirectories(out.toAbsolutePath().getParent());Files.writeString(out,Json.write(result));
        System.out.println(Json.write(result));
        deleteQuietly(dir);
    }
    /** Stop-the-world GC pauses during the run, from the JVM's own notifications. */
    static final class GcPauses {
        final LongAdder count=new LongAdder(),totalMs=new LongAdder();
        final AtomicLong maxMs=new AtomicLong();
        /** Pause intervals in epoch milliseconds. */
        final List<long[]> intervals=new ArrayList<>();
        static GcPauses install() {
            GcPauses p=new GcPauses();
            for(var bean:ManagementFactory.getGarbageCollectorMXBeans()) {
                if(!(bean instanceof javax.management.NotificationEmitter emitter))continue;
                emitter.addNotificationListener((n,h)->{
                    var info=com.sun.management.GarbageCollectionNotificationInfo.from((javax.management.openmbean.CompositeData)n.getUserData());
                    // Concurrent work and ZGC "Cycles" beans report cycle lengths, not stop-the-world pauses.
                    if(info.getGcName().contains("Concurrent")||info.getGcName().contains("Cycles"))return;
                    long ms=info.getGcInfo().getDuration();
                    p.count.increment();p.totalMs.add(ms);p.maxMs.accumulateAndGet(ms,Math::max);
                    long jvmStart=ManagementFactory.getRuntimeMXBean().getStartTime();
                    synchronized(p.intervals){p.intervals.add(new long[]{jvmStart+info.getGcInfo().getStartTime(),jvmStart+info.getGcInfo().getEndTime()});}
                },n->n.getType().equals(com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION),null);
            }
            return p;
        }
        Map<String,Object> snapshot(){return Map.of("pauses",count.sum(),"totalPauseMs",totalMs.sum(),"maxPauseMs",maxMs.get());}
    }
    /**
     * jHiccup-style stall detector: a platform thread spins and records every gap longer than 2 ms
     * between consecutive iterations, i.e. moments the JVM (safepoints) or the OS stopped it.
     * Costs one core while enabled, so it is off by default and used only for tail attribution.
     */
    static final class Hiccups {
        final List<long[]> intervals=new ArrayList<>();   // epoch ms
        volatile boolean running=true;
        Thread thread;
        static Hiccups start() {
            Hiccups h=new Hiccups();
            long offset=System.currentTimeMillis()*1_000_000-System.nanoTime();
            h.thread=Thread.ofPlatform().daemon().name("hiccup-meter").start(()->{
                long prev=System.nanoTime();
                while(h.running) {
                    long now=System.nanoTime();
                    if(now-prev>2_000_000)synchronized(h.intervals){h.intervals.add(new long[]{(prev+offset)/1_000_000,(now+offset)/1_000_000});}
                    prev=now;
                }
            });
            return h;
        }
        void stop()throws InterruptedException{running=false;thread.join();}
    }
    /**
     * Which slow workflows coincided with a GC pause, another JVM/OS stall, or neither? Compares the
     * slowest 1% of the measured window with the base rate over all measured workflows.
     */
    static Map<String,Object> attribute(long[] latencyMicros,long[] startNanos,long[] endNanos,int warm,GcPauses gc,Hiccups hiccups) {
        long offset=System.currentTimeMillis()*1_000_000-System.nanoTime();
        List<long[]> gcs,stalls;
        synchronized(gc.intervals){gcs=new ArrayList<>(gc.intervals);}
        if(hiccups==null)stalls=List.of();else synchronized(hiccups.intervals){stalls=new ArrayList<>(hiccups.intervals);}
        long[] measured=Arrays.copyOfRange(latencyMicros,warm,latencyMicros.length);Arrays.sort(measured);
        long threshold=LatencyRecorder.percentile(measured,.99);
        int tail=0,tailGc=0,tailStall=0,all=0,allGc=0,allStall=0;
        for(int i=warm;i<latencyMicros.length;i++) {
            long s=(startNanos[i]+offset)/1_000_000,e=(endNanos[i]+offset)/1_000_000;
            boolean g=overlaps(gcs,s,e),h=!g&&overlaps(stalls,s,e);
            all++;if(g)allGc++;if(h)allStall++;
            if(latencyMicros[i]>=threshold){tail++;if(g)tailGc++;if(h)tailStall++;}
        }
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("tailThresholdMicros",threshold);m.put("tailWorkflows",tail);
        m.put("tailOverlappingGcPause",tail==0?0.0:(double)tailGc/tail);
        m.put("tailOverlappingOtherStall",tail==0?0.0:(double)tailStall/tail);
        m.put("tailOverlappingNeither",tail==0?0.0:(double)(tail-tailGc-tailStall)/tail);
        m.put("baseOverlappingGcPause",all==0?0.0:(double)allGc/all);
        m.put("baseOverlappingOtherStall",all==0?0.0:(double)allStall/all);
        m.put("hiccupMeter",hiccups!=null);m.put("gcPauses",gcs.size());m.put("otherStalls",stalls.size());
        long stallMs=0,maxStall=0;for(long[] x:stalls){stallMs+=x[1]-x[0];maxStall=Math.max(maxStall,x[1]-x[0]);}
        m.put("otherStallTotalMs",stallMs);m.put("otherStallMaxMs",maxStall);
        return m;
    }
    private static boolean overlaps(List<long[]> intervals,long start,long end) {
        for(long[] x:intervals)if(x[0]<=end&&x[1]>=start)return true;
        return false;
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
