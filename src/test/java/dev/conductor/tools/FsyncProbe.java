package dev.conductor.tools;

import dev.conductor.core.Json;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Measures the device, not the engine: T threads each append 1 KiB to their own file and call
 * {@code force(true)} in a loop. If total fsyncs/s rises with T, independent journal shards can
 * flush in parallel; if it stays flat, partitioning cannot beat a single group-committing writer.
 *
 * <pre>FsyncProbe [--threads 1,2,4,8,16] [--seconds 4] [--out file.json]</pre>
 */
public final class FsyncProbe {
    public static void main(String[] args)throws Exception {
        Map<String,String> o=Benchmark.options(args);
        int seconds=Integer.parseInt(o.getOrDefault("seconds","4"));
        // append: grow the file on every write (file-size metadata must be flushed too);
        // prealloc: overwrite inside an already-sized file, as a segment-preallocating WAL would.
        // dsync / prealloc-dsync: open with DSYNC (Windows: FILE_FLAG_WRITE_THROUGH) and skip force().
        String mode=o.getOrDefault("mode","append");
        boolean prealloc=mode.startsWith("prealloc"),dsync=mode.endsWith("dsync");
        Path dir=Files.createTempDirectory("conductor-fsync-");
        List<Object> rows=new ArrayList<>();
        try {
            for(String t:o.getOrDefault("threads","1,2,4,8,16").split(",")) {
                int threads=Integer.parseInt(t);
                long[] counts=new long[threads];
                long deadline=System.nanoTime()+seconds*1_000_000_000L;
                try(ExecutorService pool=Executors.newFixedThreadPool(threads)) {
                    List<Future<?>> futures=new ArrayList<>();
                    for(int i=0;i<threads;i++){int id=i;futures.add(pool.submit(()->{
                        Path file=dir.resolve("t"+threads+"-"+id);
                        if(prealloc)try(RandomAccessFile raf=new RandomAccessFile(file.toFile(),"rw")) {
                            byte[] zeros=new byte[1<<20];for(int m=0;m<64;m++)raf.write(zeros);raf.getFD().sync();
                        }
                        Set<StandardOpenOption> options=new HashSet<>(List.of(StandardOpenOption.CREATE,StandardOpenOption.WRITE));
                        if(dsync)options.add(StandardOpenOption.DSYNC);
                        try(FileChannel ch=FileChannel.open(file,options)) {
                            ByteBuffer b=ByteBuffer.allocate(1024);long position=prealloc?0:ch.size();
                            while(System.nanoTime()<deadline) {
                                b.clear();ch.write(b,position);position+=1024;
                                if(prealloc&&position>=(64L<<20))position=0;
                                if(!dsync)ch.force(true);
                                counts[id]++;
                            }
                        }
                        return null;
                    }));}
                    for(Future<?> f:futures)f.get();
                }
                long total=Arrays.stream(counts).sum();
                Map<String,Object> row=new LinkedHashMap<>();
                row.put("threads",threads);row.put("fsyncsPerSecond",total/(double)seconds);
                row.put("meanFsyncMs",threads*seconds*1000.0/total);
                rows.add(row);System.out.println(Json.write(row));
            }
        } finally {Benchmark.deleteQuietly(dir);}
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("os",System.getProperty("os.name"));result.put("java",System.getProperty("java.version"));
        result.put("fileBytesPerWrite",1024);result.put("mode",mode);result.put("secondsPerPoint",seconds);result.put("points",rows);
        if(o.containsKey("out"))Files.writeString(Path.of(o.get("out")),Json.write(result));
    }
}
