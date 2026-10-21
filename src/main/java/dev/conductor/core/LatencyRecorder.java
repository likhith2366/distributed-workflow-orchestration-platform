package dev.conductor.core;

import java.util.*;

/** Fixed-memory ring of the most recent samples; percentiles are nearest-rank over that window. */
public final class LatencyRecorder {
    private final long[] samples;
    private long count;
    public LatencyRecorder(int capacity){samples=new long[capacity];}
    public synchronized void record(long value){samples[(int)(count++%samples.length)]=Math.max(0,value);}
    /** Records a nanosecond duration in microseconds (snapshot keys still say "Ms"; callers name them). */
    public void recordMicros(long nanos){record(nanos/1_000);}
    public Map<String,Object> snapshot(String prefix) {
        long[] copy;long total;
        synchronized(this){total=count;copy=Arrays.copyOf(samples,(int)Math.min(count,samples.length));}
        Arrays.sort(copy);
        Map<String,Object> m=new LinkedHashMap<>();
        m.put(prefix+"SampleCount",copy.length);m.put(prefix+"TotalCount",total);
        m.put(prefix+"P50Ms",percentile(copy,.50));m.put(prefix+"P99Ms",percentile(copy,.99));
        return m;
    }
    public static long percentile(long[] sorted,double p) {
        if(sorted.length==0)return 0;
        return sorted[Math.max(0,Math.min(sorted.length-1,(int)Math.ceil(sorted.length*p)-1))];
    }
}
