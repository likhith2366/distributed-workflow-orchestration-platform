package dev.conductor.journal;

import dev.conductor.core.Json;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Framed WAL: length | JSON | CRC32C. Acknowledgments follow durable writes.
 * Incomplete final frames are truncated; complete corrupt frames fail closed.
 *
 * <p>Sync modes ({@code JOURNAL_SYNC} / {@code -Dconductor.walSync}):
 * <ul>
 *   <li>{@code write-through} (default): the file is opened with {@code DSYNC}, so every write
 *       returns only once durable (Windows: {@code FILE_FLAG_WRITE_THROUGH}, issued as FUA writes;
 *       Linux: {@code O_DSYNC}). Both include the metadata needed to read the data back, such as
 *       the new file size. On the reference NVMe a durable 1 KiB append took 0.11 ms vs 0.63 ms.</li>
 *   <li>{@code flush}: write, then {@code force(true)} ({@code FlushFileBuffers} / {@code fsync}),
 *       which drains the device's whole write cache. Use it on storage that ignores FUA.</li>
 * </ul>
 */
public final class Journal implements AutoCloseable {
    private final FileChannel channel;
    private final FileLock lock;
    private boolean poisoned;
    // Lock-free counters: encode runs on the executor thread while force holds this monitor during fsync.
    private final java.util.concurrent.atomic.LongAdder flushes=new java.util.concurrent.atomic.LongAdder(),records=new java.util.concurrent.atomic.LongAdder(),
            bytes=new java.util.concurrent.atomic.LongAdder(),encodeNanos=new java.util.concurrent.atomic.LongAdder(),forceNanos=new java.util.concurrent.atomic.LongAdder();
    public static final boolean WRITE_THROUGH=syncMode().equals("write-through");
    static String syncMode() {
        String mode=System.getProperty("conductor.walSync",Objects.requireNonNullElse(System.getenv("JOURNAL_SYNC"),"write-through"));
        if(!mode.equals("write-through")&&!mode.equals("flush"))throw new IllegalArgumentException("JOURNAL_SYNC must be write-through or flush");
        return mode;
    }
    public Journal(Path path, Consumer<Map<String,Object>> replay) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Set<StandardOpenOption> options=new HashSet<>(List.of(StandardOpenOption.CREATE,StandardOpenOption.READ,StandardOpenOption.WRITE));
        if(WRITE_THROUGH)options.add(StandardOpenOption.DSYNC);
        channel=FileChannel.open(path,options);
        FileLock acquired=null;
        try {
            acquired=channel.tryLock();
            if(acquired==null)throw new IOException("Data directory already in use");
            lock=acquired;
            long offset=0, size=channel.size();
            while(offset<size) {
                if(size-offset<4){truncateDurably(offset);break;}
                ByteBuffer header=ByteBuffer.allocate(4); read(header); header.flip();
                int length=header.getInt();
                if(length<2 || length>16*1024*1024)throw new IOException("Invalid WAL frame at "+offset);
                if(size-offset<length+8L){truncateDurably(offset);break;}
                ByteBuffer body=ByteBuffer.allocate(length+4); read(body); body.flip();
                byte[] json=new byte[length]; body.get(json);
                CRC32C crc=new CRC32C();crc.update(json);
                if(body.getInt()!=(int)crc.getValue())throw new IOException("WAL checksum mismatch at "+offset);
                replay.accept(Json.obj(Json.parse(new String(json,StandardCharsets.UTF_8))));
                offset+=length+8L;
            }
            channel.position(channel.size());
        } catch(Throwable t) {
            if(acquired!=null)acquired.release();
            channel.close(); throw t;
        }
    }
    /** Drops a torn tail and makes the new length durable before any append can follow it. */
    private void truncateDurably(long offset)throws IOException {channel.truncate(offset);channel.force(true);}
    private void read(ByteBuffer b)throws IOException {
        while(b.hasRemaining())if(channel.read(b)<0)throw new EOFException();
    }
    public synchronized void append(Map<String,Object> record)throws IOException {
        appendBatch(List.of(record));
    }
    public synchronized void appendBatch(List<Map<String,Object>> batch)throws IOException {
        if(batch.isEmpty())return;
        force(encode(batch),batch.size());
    }
    /** Frames records ({@code length | JSON | CRC32C}); pure CPU, safe off the flushing thread. */
    public byte[] encode(List<Map<String,Object>> batch)throws IOException {
        long begin=System.nanoTime();
        ByteArrayOutputStream bytes=new ByteArrayOutputStream(batch.size()*1024);
        DataOutputStream output=new DataOutputStream(bytes);
        for(Map<String,Object> record:batch) {
            byte[] data=Json.write(record).getBytes(StandardCharsets.UTF_8);
            if(data.length>16*1024*1024)throw new IOException("WAL frame too large");
            CRC32C crc=new CRC32C();crc.update(data);
            output.writeInt(data.length);output.write(data);output.writeInt((int)crc.getValue());
        }
        encodeNanos.add(System.nanoTime()-begin);
        return bytes.toByteArray();
    }
    /** Appends pre-encoded frames and forces them to stable storage. Any failure poisons the journal. */
    public synchronized void force(byte[] frames,int recordCount)throws IOException {
        if(poisoned)throw new IOException("WAL failed; restart required");
        if(frames.length==0)return;
        ByteBuffer b=ByteBuffer.wrap(frames);
        long begin=System.nanoTime();
        try {
            while(b.hasRemaining())channel.write(b);
            // Write-through writes are already durable when write() returns.
            if(!WRITE_THROUGH)channel.force(true);
            long spent=System.nanoTime()-begin;
            forceNanos.add(spent);writeLatency.recordMicros(spent);flushes.increment();records.add(recordCount);bytes.add(frames.length);
        } catch(IOException e){poisoned=true;throw e;}
    }
    /** Distribution of durable-write latency, to separate device tail from queueing tail. */
    private final dev.conductor.core.LatencyRecorder writeLatency=new dev.conductor.core.LatencyRecorder(65_536);
    public Map<String,Object> writeLatency(){return writeLatency.snapshot("writeMicros");}
    public Map<String,Object> stats() {
        return Map.of("flushes",flushes.sum(),"records",records.sum(),"bytes",bytes.sum(),"sync",WRITE_THROUGH?"write-through":"flush",
                "encodeMs",encodeNanos.sum()/1_000_000,"writeAndForceMs",forceNanos.sum()/1_000_000);
    }
    public synchronized void close()throws IOException {lock.release();channel.close();}
}
