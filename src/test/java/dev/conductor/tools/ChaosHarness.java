package dev.conductor.tools;

import dev.conductor.core.*;
import dev.conductor.journal.*;
import dev.conductor.worker.EmbeddedWorkers;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Real child-JVM kills at seeded durable boundaries, followed by disk recovery. */
public final class ChaosHarness {
    static final Map<String,Object> DEF=Json.obj(Json.parse("""
        {"name":"crash-boundary","version":"1","steps":[{"id":"effect","action":"ledger","maxAttempts":20}]}
        """));
    public static void main(String[] args)throws Exception {
        if(args.length>0&&args[0].equals("child")){child(Path.of(args[1]),Integer.parseInt(args[2]));return;}
        int count=args.length>0?Integer.parseInt(args[0]):200;long seed=args.length>1?Long.parseLong(args[1]):20261020;
        Random random=new Random(seed);Path root=Files.createTempDirectory("conductor-chaos-");
        List<Long> recovery=new ArrayList<>();Map<String,Integer> boundaries=new TreeMap<>();
        long start=System.nanoTime();int completed=0,totalEffects=0;
        String java=Path.of(System.getProperty("java.home"),"bin","java").toString();
        for(int i=0;i<count;i++) {
            int phase=random.nextInt(4);boundaries.merge(List.of("after-start","after-claim","after-effect","after-completion").get(phase),1,Integer::sum);
            Path dir=root.resolve("run-"+i);Files.createDirectories(dir);
            Process process=new ProcessBuilder(java,"-cp",System.getProperty("java.class.path"),ChaosHarness.class.getName(),
                    "child",dir.toString(),Integer.toString(phase)).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try {
                try(ExecutorService reader=Executors.newVirtualThreadPerTaskExecutor()) {
                    String line=reader.submit(()->new BufferedReader(new InputStreamReader(process.getInputStream())).readLine()).get(15,TimeUnit.SECONDS);
                    if(!"KILL_READY".equals(line))throw new AssertionError("Child failed before crash boundary");
                }
                long death=System.nanoTime();process.destroyForcibly();
                if(!process.waitFor(10,TimeUnit.SECONDS))throw new AssertionError("Child did not terminate");
                try(JournalEngine engine=openAfterKill(dir);JournalLedger ledger=new JournalLedger(dir)) {
                    String id=Json.str(Json.obj(engine.list(1).get(0)),"id","");
                    while(!Workflow.terminal(Json.str(engine.get(id),"status",""))) {
                        if(System.nanoTime()-death>10_000_000_000L)throw new AssertionError("Recovery timeout");
                        Claim c=engine.claim("recovery",System.currentTimeMillis(),3000);
                        if(c==null){Thread.sleep(2);continue;}
                        if(!engine.complete(c,Activities.execute(c,ledger),null,System.currentTimeMillis()))
                            throw new AssertionError("Recovery completion rejected");
                    }
                    if(!engine.get(id).get("status").equals("SUCCEEDED"))throw new AssertionError("Lost workflow");
                    if(ledger.size()!=1)throw new AssertionError("Incorrect side-effect count");
                    totalEffects+=ledger.size();completed++;
                }
                recovery.add((System.nanoTime()-death)/1_000_000);
            } finally {if(process.isAlive()){process.destroyForcibly();process.waitFor();}}
            if((i+1)%25==0)System.out.println("Recovered "+(i+1)+"/"+count+" killed processes");
        }
        Collections.sort(recovery);
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("timestamp",Instant.now().toString());result.put("seed",seed);result.put("processKills",count);
        result.put("completed",completed);result.put("downstreamEffects",totalEffects);result.put("duplicateEffects",totalEffects-completed);
        result.put("boundaries",boundaries);result.put("leaseMs",150);result.put("recoveryP50Ms",recovery.get(count/2));
        result.put("recoveryP99Ms",recovery.get(Math.min(count-1,(int)Math.ceil(count*.99)-1)));
        result.put("seconds",(System.nanoTime()-start)/1e9);result.put("os",System.getProperty("os.name"));
        result.put("java",System.getProperty("java.version"));
        result.put("scope","Local child JVM killed after acknowledged boundaries; coordinator and worker share child; no network partition or power-loss test");
        Files.createDirectories(Path.of("benchmarks"));Files.writeString(Path.of(args.length>2?args[2]:"benchmarks/chaos.json"),Json.write(result));
        System.out.println(Json.write(result));
    }
    private static void child(Path dir,int phase)throws Exception {
        try(JournalEngine e=new JournalEngine(dir);JournalLedger ledger=new JournalLedger(dir)) {
            e.start(DEF,"crash",System.currentTimeMillis());if(phase==0)pause();
            var c=e.claim("child",System.currentTimeMillis(),150);if(phase==1)pause();
            Object result=Activities.execute(c,ledger);if(phase==2)pause();
            if(!e.complete(c,result,null,System.currentTimeMillis()))throw new AssertionError("Child lease expired");
            pause();
        }
    }
    private static void pause()throws InterruptedException {System.out.println("KILL_READY");System.out.flush();Thread.sleep(60_000);}
    private static JournalEngine openAfterKill(Path dir)throws Exception {
        long deadline=System.nanoTime()+5_000_000_000L;
        while(true)try{return new JournalEngine(dir);}
        catch(IOException e) {
            if(!"Data directory already in use".equals(e.getMessage())||System.nanoTime()>deadline)throw e;
            // Windows may report process exit just before kernel file locks are released.
            Thread.sleep(10);
        }
    }
}
