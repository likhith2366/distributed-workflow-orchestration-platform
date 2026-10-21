package dev.conductor.tools;

import dev.conductor.core.*;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Multi-process benchmark: one coordinator JVM, {@code processes} independent worker JVMs, and a load
 * generator, all talking HTTP over loopback. Separate processes and sockets, but one machine: this
 * measures protocol and coordinator costs, not cross-host network latency.
 *
 * <pre>ClusterBenchmark --jar target/conductor.jar [--processes P] [--workers-per-process K]
 *     [--workflows N] [--clients C] [--activity-ms A] [--out file.json]</pre>
 */
public final class ClusterBenchmark {
    static final String TOKEN="cluster-benchmark-token-with-32-chars";
    public static void main(String[] args)throws Exception {
        Map<String,String> o=Benchmark.options(args);
        Path jar=Path.of(o.getOrDefault("jar","target/conductor.jar"));
        int processes=Integer.parseInt(o.getOrDefault("processes","2"));
        int perProcess=Integer.parseInt(o.getOrDefault("workers-per-process","8"));
        int count=Integer.parseInt(o.getOrDefault("workflows","2000"));
        int clients=Integer.parseInt(o.getOrDefault("clients","64"));
        long activityMs=Long.parseLong(o.getOrDefault("activity-ms","0"));
        String java=Path.of(System.getProperty("java.home"),"bin","java").toString();
        int port;try(ServerSocket s=new ServerSocket(0,1,InetAddress.getLoopbackAddress())){port=s.getLocalPort();}
        String base="http://127.0.0.1:"+port;
        Path data=Files.createTempDirectory("conductor-cluster-");
        List<Process> children=new ArrayList<>();
        try {
            Process coordinator=spawn(java,jar,data.resolve("coordinator.log"),Map.of("PORT",Integer.toString(port),
                    "DATA_DIR",data.toString(),"WORKERS","0","CONDUCTOR_MAX_CONCURRENT_REQUESTS","4096"),"-Xmx1g");
            children.add(coordinator);
            HttpClient http=HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
            awaitHealthy(http,base);
            for(int i=0;i<processes;i++)children.add(spawn(java,jar,data.resolve("worker-"+i+".log"),Map.of("CONDUCTOR_ROLE","worker",
                    "WORKERS",Integer.toString(perProcess),"COORDINATOR_URL",base),"-Xmx384m"));
            String latency=activityMs>0?",\"latencyMs\":"+activityMs:"";
            String definition="{\"name\":\"cluster\",\"version\":\"1\",\"steps\":["
                    +"{\"id\":\"reserve\",\"action\":\"ledger\",\"maxAttempts\":20"+latency+"},"
                    +"{\"id\":\"charge\",\"action\":\"ledger\",\"maxAttempts\":20,\"dependsOn\":[\"reserve\"]"+latency+"},"
                    +"{\"id\":\"notify\",\"action\":\"echo\",\"maxAttempts\":20,\"dependsOn\":[\"charge\"]"+latency+"}]}";
            // Warm-up: JIT-compile every process before measuring; also proves all workers connected.
            drive(http,base,definition,Math.max(200,processes*perProcess*4),clients,"warm");
            long cpuBefore=cpu(children);
            long begin=System.nanoTime();
            long[][] timing=drive(http,base,definition,count,clients,"run");
            double seconds=(System.nanoTime()-begin)/1e9;
            double cpuCores=(cpu(children)-cpuBefore)/1e9/seconds;
            long[] latencies=timing[0],finished=timing[1];
            Arrays.sort(latencies);Arrays.sort(finished);
            int lo=count/10,hi=count*9/10;
            Map<String,Object> metrics=Json.obj(Json.parse(send(http,base,"GET","/api/metrics",null).body()));
            Map<String,Object> result=new LinkedHashMap<>();
            result.put("timestamp",Instant.now().toString());
            result.put("scope","1 coordinator JVM + "+processes+" worker JVMs + load generator on one machine, HTTP over loopback; not AWS");
            result.put("java",System.getProperty("java.version"));result.put("os",System.getProperty("os.name"));
            result.put("availableProcessors",Runtime.getRuntime().availableProcessors());
            result.put("processes",processes);result.put("workersPerProcess",perProcess);result.put("workers",processes*perProcess);
            result.put("workflows",count);result.put("clients",clients);result.put("activityMs",activityMs);result.put("seconds",seconds);
            result.put("workflowsPerSecond",count/seconds);
            result.put("steadyWorkflowsPerSecond",(hi-lo)/((finished[hi]-finished[lo])/1e9));
            result.put("workflowP50Ms",LatencyRecorder.percentile(latencies,.50));
            result.put("workflowP99Ms",LatencyRecorder.percentile(latencies,.99));
            result.put("allProcessesCpuCores",cpuCores);
            result.put("coordinatorMetrics",metrics);
            Path out=Path.of(o.getOrDefault("out","benchmarks/cluster-latest.json"));
            Files.createDirectories(out.toAbsolutePath().getParent());Files.writeString(out,Json.write(result));
            System.out.println(Json.write(result));
        } finally {
            for(Process p:children)p.destroyForcibly();
            for(Process p:children)p.waitFor(10,TimeUnit.SECONDS);
            Benchmark.deleteQuietly(data);
        }
    }
    static long[][] drive(HttpClient http,String base,String definition,int count,int clients,String prefix)throws Exception {
        long[] latencies=new long[count],finished=new long[count];
        AtomicInteger next=new AtomicInteger();
        String run=prefix+"-"+UUID.randomUUID().toString().substring(0,8)+"-";
        try(ExecutorService pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures=new ArrayList<>();
            for(int c=0;c<clients;c++)futures.add(pool.submit(()->{
                for(int i;(i=next.getAndIncrement())<count;) {
                    long start=System.nanoTime();
                    HttpResponse<String> started=send(http,base,"POST","/api/executions",definition,"Idempotency-Key",run+i);
                    if(started.statusCode()!=202)throw new IllegalStateException("start HTTP "+started.statusCode()+" "+started.body());
                    String id=Json.str(Json.obj(Json.parse(started.body())),"id","");
                    String status;
                    do status=Json.str(Json.obj(Json.parse(send(http,base,"GET","/api/executions/"+id+"?waitMs=30000",null).body())),"status","");
                    while(!Workflow.terminal(status));
                    if(!status.equals("SUCCEEDED"))throw new IllegalStateException("Execution "+id+" ended "+status);
                    long end=System.nanoTime();latencies[i]=(end-start)/1_000_000;finished[i]=end;
                }
                return null;
            }));
            for(Future<?> f:futures)f.get(15,TimeUnit.MINUTES);
        }
        return new long[][]{latencies,finished};
    }
    static HttpResponse<String> send(HttpClient http,String base,String method,String path,String body,String... headers)throws Exception {
        HttpRequest.Builder b=HttpRequest.newBuilder(URI.create(base+path)).timeout(Duration.ofSeconds(60))
                .header("Authorization","Bearer "+TOKEN).header("Content-Type","application/json")
                .method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(body));
        for(int i=0;i<headers.length;i+=2)b.header(headers[i],headers[i+1]);
        return http.send(b.build(),HttpResponse.BodyHandlers.ofString());
    }
    static Process spawn(String java,Path jar,Path log,Map<String,String> env,String heap)throws IOException {
        ProcessBuilder pb=new ProcessBuilder(java,heap,"-XX:MaxGCPauseMillis=5","-jar",jar.toAbsolutePath().toString())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        pb.environment().putAll(env);
        pb.environment().put("API_TOKEN",TOKEN);pb.environment().put("BIND_ADDRESS","127.0.0.1");
        return pb.start();
    }
    static void awaitHealthy(HttpClient http,String base)throws Exception {
        long deadline=System.nanoTime()+90_000_000_000L;
        while(true) {
            try{if(send(http,base,"GET","/healthz",null).statusCode()==200)return;}catch(IOException ignored){}
            if(System.nanoTime()>deadline)throw new IllegalStateException("coordinator did not start");
            Thread.sleep(200);
        }
    }
    static long cpu(List<Process> processes) {
        long total=ProcessHandle.current().info().totalCpuDuration().map(Duration::toNanos).orElse(0L);
        for(Process p:processes)total+=p.info().totalCpuDuration().map(Duration::toNanos).orElse(0L);
        return total;
    }
}
