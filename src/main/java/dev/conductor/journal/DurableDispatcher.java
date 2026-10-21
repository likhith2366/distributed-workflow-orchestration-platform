package dev.conductor.journal;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Single-writer command mailbox with group commit, optionally pipelined.
 *
 * <p>An <em>executor</em> thread runs commands one at a time against speculative in-memory state
 * and, after each, asks the {@link Log} to {@link Log#stage stage} the records it produced into the
 * open batch. A <em>flusher</em> thread repeatedly takes the whole open batch, makes it durable with
 * one {@link Log#force force}, runs its after-durable hooks (publishing state to readers) in order,
 * and only then replies to its commands. Execution of the next batch therefore overlaps the
 * {@code fsync} of the previous one. The flusher takes a batch when the executor has drained the
 * queue or the batch is full, so a burst of commands is never split across two flushes.
 *
 * <p>Safety is unchanged from plain group commit: batches flush in execution order, and no reply
 * (including to a read) is released before its batch, and so every earlier batch, is durable. A
 * flush failure permanently poisons the dispatcher, failing that batch and everything after it;
 * speculative state is then discarded by restarting from the WAL.
 */
public final class DurableDispatcher implements AutoCloseable {
    @FunctionalInterface public interface Operation<T> { T run() throws Exception; }
    @FunctionalInterface public interface Flush { void run() throws IOException; }
    /** Durable effects of one executed command: encoded frames plus a hook to run once durable. */
    public record Staged(byte[] frames,int records,Runnable afterDurable) {}
    public interface Log {
        /** Executor thread, after every command (even one that wrote nothing). Never null. */
        Staged stage()throws IOException;
        /** Flusher thread: write and force the concatenated frames of one batch. */
        void force(byte[] frames,int records)throws IOException;
    }

    private static final class Command<T> {
        final Operation<T> operation;
        final CompletableFuture<T> reply=new CompletableFuture<>();
        T result; Exception error;
        Command(Operation<T> operation){this.operation=operation;}
        void execute(){try{result=operation.run();}catch(Exception e){error=e;}}
        void finish(){if(error==null)reply.complete(result);else reply.completeExceptionally(error);}
        void fail(IOException e){reply.completeExceptionally(e);}
    }
    private static final class Batch {
        final List<Command<?>> commands=new ArrayList<>();
        final ByteArrayOutputStream frames=new ByteArrayOutputStream();
        final List<Runnable> after=new ArrayList<>();
        int records;
    }

    private final BlockingQueue<Command<?>> queue=new LinkedBlockingQueue<>();
    private final Command<Void> stop=new Command<>(()->null);
    private final Object lock=new Object();
    private Batch open=new Batch();          // guarded by lock
    private boolean draining;                // guarded by lock: executor has stopped
    private boolean closed;                  // guarded by this
    private volatile IOException failed;
    private final Log log;
    private final int maxBatch;
    private final Thread executor,flusher;
    /**
     * Default: classic group commit (execute what is queued, then flush inline). Measured best below
     * saturation, where overlapping execution with fsync only produces more, smaller flushes.
     * {@code -Dconductor.pipelinedCommit=true} overlaps them: +5% throughput and a lower p99 at 128
     * workers on the reference laptop, about -10% at 4 workers. See docs/scaling.md.
     */
    private final boolean pipelined=Boolean.getBoolean("conductor.pipelinedCommit");
    // Utilisation, for explaining where throughput stops scaling.
    private volatile long batches,commands,executeNanos,flushNanos,maxSeen;

    public DurableDispatcher(String name,Log log) {
        this.log=log;
        maxBatch=Integer.getInteger("conductor.batchSize",256);
        if(maxBatch<1||maxBatch>4096)throw new IllegalArgumentException("batchSize must be 1..4096");
        flusher=pipelined?Thread.ofPlatform().name(name+"-flush").start(this::flushLoop):null;
        executor=Thread.ofPlatform().name(name).start(this::executeLoop);
    }
    /** A single-stage dispatcher whose flush runs once per batch (used by tests and simple logs). */
    public DurableDispatcher(String name,Flush flush) {
        this(name,new Log() {
            public Staged stage(){return new Staged(new byte[0],1,null);}
            public void force(byte[] frames,int records)throws IOException{flush.run();}
        });
    }

    public <T> T call(Operation<T> operation)throws IOException {
        Command<T> command=new Command<>(operation);
        synchronized(this) {
            if(failed!=null)throw new IOException("Durability failure; restart required",failed);
            if(closed)throw new IOException("Dispatcher is closed");
            queue.add(command);
        }
        // An interrupted caller must not assume its command was cancelled.
        try{return command.reply.get();}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new IOException("Interrupted; command outcome may be durable",e);}
        catch(ExecutionException e) {
            Throwable cause=e.getCause();
            if(cause instanceof IOException io)throw io;
            if(cause instanceof RuntimeException re)throw re;
            throw new IOException(cause);
        }
    }
    public <T> T read(Operation<T> operation) {
        try{return call(operation);}catch(IOException e){throw new UncheckedIOException(e);}
    }

    private void executeLoop() {
        try {
            while(true) {
                Command<?> command=queue.take();
                if(command==stop)break;
                if(failed!=null){command.fail(failed);continue;}
                long begin=System.nanoTime();
                command.execute();
                Staged staged=log.stage();
                executeNanos+=System.nanoTime()-begin;
                synchronized(lock) {
                    // Backpressure: never let the open batch grow without bound behind a slow disk.
                    while(open.commands.size()>=maxBatch&&failed==null)lock.wait();
                    if(failed!=null){command.fail(failed);continue;}
                    open.commands.add(command);
                    if(staged.frames().length>0)open.frames.writeBytes(staged.frames());
                    open.records+=staged.records();
                    if(staged.afterDurable()!=null)open.after.add(staged.afterDurable());
                    lock.notifyAll();
                }
                // Sequential mode: like classic group commit, flush once nothing more is queued.
                if(!pipelined&&(queue.isEmpty()||open.commands.size()>=maxBatch))flushOpenBatch();
            }
        } catch(Throwable t) {
            poison(t instanceof IOException io?io:new IOException("Writer failed",t));
        } finally {
            if(!pipelined)flushOpenBatch();
            synchronized(lock){draining=true;lock.notifyAll();}
        }
    }
    private void flushLoop() {
        while(true) {
            synchronized(lock) {
                // Burst-aware: take the batch once the executor has drained what is already queued
                // (or the batch is full), so overlapping execution never splits a burst in two.
                try{while(!draining&&(open.commands.isEmpty()||!queue.isEmpty()&&open.commands.size()<maxBatch))lock.wait();}
                catch(InterruptedException e){Thread.currentThread().interrupt();return;}
                if(open.commands.isEmpty())return;
            }
            flushOpenBatch();
        }
    }
    /** Takes the whole open batch, forces it, publishes it, and replies to its commands. */
    private void flushOpenBatch() {
        Batch batch;
        synchronized(lock) {
            if(open.commands.isEmpty())return;
            batch=open;open=new Batch();lock.notifyAll();
        }
        if(failed!=null){batch.commands.forEach(c->c.fail(failed));return;}
        try {
            long begin=System.nanoTime();
            if(batch.records>0)log.force(batch.frames.toByteArray(),batch.records);
            flushNanos+=System.nanoTime()-begin;
            batch.after.forEach(Runnable::run);
        } catch(Throwable t) {
            poison(t instanceof IOException io?io:new IOException("Flush failed",t));
            batch.commands.forEach(c->c.fail(failed));
            return;
        }
        batch.commands.forEach(Command::finish);
        batches++;commands+=batch.commands.size();
        if(batch.commands.size()>maxSeen)maxSeen=batch.commands.size();
    }
    private void poison(IOException e) {
        synchronized(this){if(failed==null)failed=e;}
        synchronized(lock){lock.notifyAll();}
        boolean stopQueued=false;
        Command<?> c;
        while((c=queue.poll())!=null)if(c==stop)stopQueued=true;else c.fail(failed);
        if(stopQueued)queue.add(stop);
    }

    public boolean healthy(){return failed==null;}
    /** Batch sizes, executor time (commands + staging) and flusher time (write + force). */
    public Map<String,Object> stats() {
        long b=batches,c=commands;
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("batches",b);m.put("commands",c);m.put("meanBatch",b==0?0.0:(double)c/b);m.put("maxBatch",maxSeen);
        m.put("executeMs",executeNanos/1_000_000);m.put("flushMs",flushNanos/1_000_000);
        return m;
    }
    @Override public void close()throws IOException {
        synchronized(this){if(!closed){closed=true;queue.add(stop);}}
        try{executor.join();if(flusher!=null)flusher.join();}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new IOException(e);}
    }
}
