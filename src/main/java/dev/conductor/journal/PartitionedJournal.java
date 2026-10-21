package dev.conductor.journal;

import dev.conductor.core.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Partitions executions across independent journal shards: each shard has its own WAL, writer
 * thread and group commit, so durable flushes proceed in parallel instead of queueing behind one
 * writer. This is the same move as partitioning by execution in DynamoDB, applied locally.
 *
 * <p>Routing is by hash. An idempotency key picks a shard, and that shard mints an execution ID
 * hashing back to itself, so both {@code start(key)} and every later {@code get/complete(id)} land
 * on the owning shard without a lookup table. Workers prefer a home shard and steal from the rest.
 *
 * <p>Layout: one shard keeps the original {@code events.wal}/{@code ledger.wal} at the data-dir
 * root; N shards use {@code shard-0..N-1/}. Opening a directory with a different shard count fails.
 */
public final class PartitionedJournal {
    private PartitionedJournal() {}

    /** Engine and its downstream example ledger, opened with the same shard count. */
    public record Store(LocalEngine engine,EffectLedger ledger) implements AutoCloseable {
        @Override public void close()throws IOException {
            try{ledger.close();}finally{engine.close();}
        }
    }

    public static Store open(Path dir,int shards)throws IOException {
        if(shards<1||shards>64)throw new IllegalArgumentException("journal shards must be 1..64");
        checkLayout(dir,shards);
        if(shards==1) {
            JournalEngine engine=new JournalEngine(dir);
            try{return new Store(engine,new JournalLedger(dir));}
            catch(IOException|RuntimeException e){engine.close();throw e;}
        }
        WorkSignal signal=new WorkSignal();
        JournalEngine[] engines=new JournalEngine[shards];
        JournalLedger[] ledgers=new JournalLedger[shards];
        try {
            for(int i=0;i<shards;i++) {
                int shard=i;
                Path d=dir.resolve("shard-"+i);
                engines[i]=new JournalEngine(d,signal,()->idFor(shard,shards),"engine-writer-"+i);
                ledgers[i]=new JournalLedger(d);
            }
        } catch(IOException|RuntimeException e) {
            for(int i=0;i<shards;i++) {
                try{if(ledgers[i]!=null)ledgers[i].close();if(engines[i]!=null)engines[i].close();}catch(IOException ignored){}
            }
            throw e;
        }
        return new Store(new Engine(engines,signal),new Ledger(ledgers));
    }

    static int shardOf(String key,int shards) {
        int h=key.hashCode()*0x9E3779B9;
        return Math.floorMod(h^(h>>>16),shards);
    }
    private static String idFor(int shard,int shards) {
        while(true){String id=UUID.randomUUID().toString();if(shardOf(id,shards)==shard)return id;}
    }
    private static void checkLayout(Path dir,int shards)throws IOException {
        boolean root=Files.exists(dir.resolve("events.wal"));
        int existing=0;
        while(Files.isDirectory(dir.resolve("shard-"+existing)))existing++;
        if(shards==1&&existing>0||shards>1&&root||existing>0&&existing!=shards)
            throw new IOException("Data directory was written with "+(root?1:existing)+" journal shard(s); configured "+shards);
    }

    public static final class Engine implements LocalEngine {
        private final JournalEngine[] shards;
        private final WorkSignal signal;
        Engine(JournalEngine[] shards,WorkSignal signal){this.shards=shards;this.signal=signal;}
        private JournalEngine of(String key){return shards[shardOf(key,shards.length)];}

        @Override public String start(Map<String,Object> definition,String key,long now)throws IOException {
            Workflow.requireKey(key);
            return of(key).start(definition,key,now);
        }
        @Override public Claim claim(String owner,long now,long leaseMs)throws IOException {
            int home=shardOf(owner,shards.length);
            for(int i=0;i<shards.length;i++) {
                Claim c=shards[(home+i)%shards.length].claim(owner,now,leaseMs);
                if(c!=null)return c;
            }
            return null;
        }
        @Override public Handoff completeAndClaim(Claim c,Object result,String error,long now,String owner,long leaseMs)throws IOException {
            Handoff h=of(c.execution()).completeAndClaim(c,result,error,now,owner,leaseMs);
            return h.next()!=null?h:new Handoff(h.accepted(),claim(owner,now,leaseMs));
        }
        @Override public boolean heartbeat(Claim c,long now,long leaseMs)throws IOException{return of(c.execution()).heartbeat(c,now,leaseMs);}
        @Override public boolean complete(Claim c,Object result,String error,long now)throws IOException{return of(c.execution()).complete(c,result,error,now);}
        @Override public void signal(String id,String step,long now)throws IOException{of(id).signal(id,step,now);}
        @Override public void cancel(String id,long now)throws IOException{of(id).cancel(id,now);}
        @Override public Map<String,Object> get(String id){return of(id).get(id);}
        @Override public Map<String,Object> awaitTerminal(String id,long timeoutMs)throws InterruptedException{return of(id).awaitTerminal(id,timeoutMs);}
        @Override public List<Object> list(int limit) {
            List<Object> all=new ArrayList<>();
            for(JournalEngine s:shards)all.addAll(s.list(limit));
            all.sort(Comparator.comparingLong((Object r)->Json.num(Json.obj(r),"createdAt",0)).reversed());
            return all.subList(0,Math.min(limit,all.size()));
        }
        @Override public Map<String,Object> stats() {
            long executions=0,active=0,transitions=0;
            Map<String,Long> statuses=new TreeMap<>(),journal=new TreeMap<>(),writer=new TreeMap<>();
            for(JournalEngine s:shards) {
                Map<String,Object> m=s.stats();
                executions+=Json.num(m,"executions",0);active+=Json.num(m,"active",0);transitions+=Json.num(m,"durableTransitions",0);
                Json.obj(m.get("statuses")).forEach((k,v)->statuses.merge(k,((Number)v).longValue(),Long::sum));
                Json.obj(m.get("journal")).forEach((k,v)->{if(v instanceof Number n)journal.merge(k,n.longValue(),Long::sum);});
                // Only counters merge across shards; per-shard percentiles and breakdowns do not add up.
                Json.obj(m.get("writer")).forEach((k,v)->{
                    if(v instanceof Number n&&Set.of("batches","commands","executeMs","flushMs","maxBatch").contains(k))
                        writer.merge(k,n.longValue(),k.equals("maxBatch")?Math::max:Long::sum);
                });
            }
            Map<String,Object> w=new LinkedHashMap<>(writer);
            w.put("meanBatch",writer.getOrDefault("batches",0L)==0?0.0:(double)writer.get("commands")/writer.get("batches"));
            Map<String,Object> m=new LinkedHashMap<>();
            m.put("backend",backend());m.put("shards",shards.length);m.put("executions",executions);m.put("active",active);
            m.put("durableTransitions",transitions);m.put("statuses",statuses);m.put("journal",journal);m.put("writer",w);
            return m;
        }
        @Override public boolean healthy(){for(JournalEngine s:shards)if(!s.healthy())return false;return true;}
        @Override public String backend(){return "journal";}
        @Override public long workGeneration(){return signal.generation();}
        @Override public void awaitWork(long seenGeneration,long maxWaitMs)throws InterruptedException {
            long due=Long.MAX_VALUE;
            for(JournalEngine s:shards)due=Math.min(due,s.nextDue());
            signal.await(seenGeneration,Math.min(maxWaitMs,Math.max(0,due-System.currentTimeMillis())));
        }
        @Override public void close()throws IOException {
            IOException first=null;
            for(JournalEngine s:shards)try{s.close();}catch(IOException e){if(first==null)first=e;}
            if(first!=null)throw first;
        }
    }

    public static final class Ledger implements EffectLedger {
        private final JournalLedger[] shards;
        Ledger(JournalLedger[] shards){this.shards=shards;}
        @Override public Object apply(String key,Object value)throws IOException{return shards[shardOf(key,shards.length)].apply(key,value);}
        @Override public long size(){long n=0;for(JournalLedger s:shards)n+=s.size();return n;}
        public Map<String,Object> stats() {
            Map<String,Long> total=new TreeMap<>();
            for(JournalLedger s:shards)s.stats().forEach((k,v)->{if(v instanceof Number n)total.merge(k,n.longValue(),Long::sum);});
            return new LinkedHashMap<>(total);
        }
        @Override public void close()throws IOException {
            IOException first=null;
            for(JournalLedger s:shards)try{s.close();}catch(IOException e){if(first==null)first=e;}
            if(first!=null)throw first;
        }
    }
}
