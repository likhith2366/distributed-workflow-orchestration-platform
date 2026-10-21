package dev.conductor.worker;

import dev.conductor.core.*;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Independently deployable worker for the journal backend. It long-polls the coordinator for
 * claims, renews its lease once per second, and reports results over the trusted worker API.
 * The coordinator remains the single durable writer and the only clock that decides lease validity.
 */
public final class RemoteWorker implements AutoCloseable {
    private final HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ScheduledExecutorService heartbeat=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("remote-heartbeat").factory());
    private final ExecutorService loops=Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean stopping=new AtomicBoolean();
    private final String endpoint,token;
    public RemoteWorker(String endpoint,String token,int concurrency) {
        if(concurrency<1||concurrency>256)throw new IllegalArgumentException("WORKERS must be 1..256");
        this.endpoint=endpoint.replaceAll("/$","");this.token=token;
        String process=UUID.randomUUID().toString();
        for(int i=0;i<concurrency;i++) {
            String owner=process+"/"+i;
            loops.submit(()->{try{loop(owner);}catch(InterruptedException e){Thread.currentThread().interrupt();}});
        }
    }
    private Map<String,Object> call(String action,Map<String,Object> body,Duration timeout)throws Exception {
        HttpRequest request=HttpRequest.newBuilder(URI.create(endpoint+"/api/worker/"+action))
                .timeout(timeout).header("Content-Type","application/json")
                .header("Authorization","Bearer "+token).POST(HttpRequest.BodyPublishers.ofString(Json.write(body))).build();
        HttpResponse<String> response=client.send(request,HttpResponse.BodyHandlers.ofString());
        if(response.statusCode()!=200)throw new IllegalStateException("Coordinator HTTP "+response.statusCode());
        return Json.obj(Json.parse(response.body()));
    }
    private void loop(String owner)throws InterruptedException {
        Map<String,Object> handedOff=Map.of();
        while(!stopping.get()&&!Thread.currentThread().isInterrupted()) {
            try {
                Map<String,Object> wire=!handedOff.isEmpty()?handedOff:call("claim",Map.of("owner",owner,"waitMs",10_000L),Duration.ofSeconds(20));
                handedOff=Map.of();
                if(wire.isEmpty())continue;
                Claim c=Claim.fromWire(wire);
                ScheduledFuture<?> renew=heartbeat.scheduleAtFixedRate(()->{
                    try{call("heartbeat",Map.of("claim",wire),Duration.ofSeconds(5));}
                    catch(Exception e){System.err.println("Heartbeat unavailable: "+e.getMessage());}
                },1,1,TimeUnit.SECONDS);
                Object result=null;String failure=null;
                // Effects go to the coordinator's ledger, keyed by the claim's idempotency key.
                EffectLedger ledger=new EffectLedger() {
                    public Object apply(String key,Object value)throws java.io.IOException {
                        try{return call("effect",Map.of("claim",wire,"value",value),Duration.ofSeconds(10)).get("result");}
                        catch(java.io.IOException|RuntimeException e){throw e;}
                        catch(Exception e){throw new java.io.IOException(e);}
                    }
                    public long size(){throw new UnsupportedOperationException();}
                    public void close(){}
                };
                try{result=Activities.execute(c,ledger);}
                catch(InterruptedException e){throw e;}
                  catch(Exception e){failure=Objects.toString(e.getMessage(),e.getClass().getSimpleName());}
                finally{renew.cancel(false);}
                Map<String,Object> completion=new LinkedHashMap<>();
                completion.put("claim",wire);completion.put("result",result);completion.put("error",failure);completion.put("next",true);
                // The coordinator completes this task and claims our next one in the same durable commit.
                Object next=call("complete",completion,Duration.ofSeconds(10)).get("next");
                if(next instanceof Map<?,?>)handedOff=Json.obj(next);
            } catch(InterruptedException e){throw e;}
              catch(Exception e) {
                  if(stopping.get())return;
                  System.err.println("Worker retry: "+e.getMessage());Thread.sleep(250);
              }
        }
    }
    @Override public void close() {
        stopping.set(true);loops.shutdownNow();heartbeat.shutdownNow();
        try{loops.awaitTermination(5,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}
    }
}
