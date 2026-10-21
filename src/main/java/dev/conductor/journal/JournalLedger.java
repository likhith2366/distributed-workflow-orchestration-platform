package dev.conductor.journal;

import dev.conductor.core.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Example downstream system: atomically persists key + effect together in its own journal. */
public final class JournalLedger implements EffectLedger {
    private final Map<String,Object> effects=new HashMap<>();
    private final Journal journal;
    private final DurableDispatcher dispatcher;
    private final List<Map<String,Object>> pending=new ArrayList<>();
    private final AtomicLong committed=new AtomicLong();
    public JournalLedger(Path dir)throws IOException {
        journal=new Journal(dir.resolve("ledger.wal"),r->{effects.put(Json.str(r,"key",""),r.get("value"));committed.incrementAndGet();});
        dispatcher=new DurableDispatcher("ledger-writer",new DurableDispatcher.Log() {
            public DurableDispatcher.Staged stage()throws IOException {
                if(pending.isEmpty())return new DurableDispatcher.Staged(new byte[0],0,null);
                int n=pending.size();byte[] frames=journal.encode(pending);pending.clear();
                return new DurableDispatcher.Staged(frames,n,()->committed.addAndGet(n));
            }
            public void force(byte[] frames,int records)throws IOException{journal.force(frames,records);}
        });
    }
    @Override public Object apply(String key,Object value)throws IOException {
        Object owned=Json.deepCopy(value);
        return dispatcher.call(()->doApply(key,owned));
    }
    private Object doApply(String key,Object value) {
        if(effects.containsKey(key)) {
            Object existing=effects.get(key);
            if(!Json.write(existing).equals(Json.write(value)))throw new IllegalArgumentException("Idempotency payload mismatch");
            return Json.deepCopy(existing);
        }
        Map<String,Object> record=new LinkedHashMap<>();record.put("key",key);record.put("value",value);
        pending.add(record);effects.put(key,value);return Json.deepCopy(value);
    }
    @Override public long size(){return committed.get();}
    public Map<String,Object> stats() {
        Map<String,Object> m=new LinkedHashMap<>(journal.stats());m.put("writer",dispatcher.stats());return m;
    }
    @Override public void close()throws IOException {dispatcher.close();journal.close();}
}
