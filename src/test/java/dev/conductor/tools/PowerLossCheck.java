package dev.conductor.tools;

import dev.conductor.core.Json;
import dev.conductor.journal.Journal;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Power-loss verification for the journal on real hardware (the "diskchecker" method). The only way
 * to know whether a drive honours write-through/flush is to cut its power while acknowledged writes
 * are in flight and check what survived; the OS documentation describes requests, not the device.
 *
 * <pre>
 * On a second machine that keeps power:   PowerLossCheck listen --port 9123
 * On the machine under test:              PowerLossCheck write  --dir D:\plc --host other-host --port 9123
 *   (optionally JOURNAL_SYNC=flush to test that mode), then pull the power cord / hold the power button.
 * After reboot, on the machine under test: PowerLossCheck verify --dir D:\plc --run RUN --acked N
 *   RUN and N are the last line the listener printed for that run.
 * </pre>
 *
 * The writer uses the production {@link Journal} (same framing, checksums and sync mode). A record
 * is reported to the listener only after {@code appendBatch} returned, i.e. after the journal
 * acknowledged it as durable. Verification passes only if every acknowledged sequence number is
 * present after replay, contiguously. Records beyond N may or may not survive; that is allowed.
 */
public final class PowerLossCheck {
    public static void main(String[] args)throws Exception {
        if(args.length==0)throw new IllegalArgumentException("usage: listen|write|verify [--options]");
        Map<String,String> o=Benchmark.options(Arrays.copyOfRange(args,1,args.length));
        switch(args[0]) {
            case "listen" -> listen(Integer.parseInt(o.getOrDefault("port","9123")));
            case "write" -> write(Path.of(o.get("dir")),o.get("host"),Integer.parseInt(o.getOrDefault("port","9123")));
            case "verify" -> System.exit(verify(Path.of(o.get("dir")),o.get("run"),Long.parseLong(o.get("acked")))?0:1);
            default -> throw new IllegalArgumentException("unknown mode "+args[0]);
        }
    }

    static void listen(int port)throws IOException {
        try(ServerSocket server=new ServerSocket(port)) {
            System.out.println("listening on "+port+"; the last line per run is the acknowledged high-water mark");
            while(true) {
                Socket socket=server.accept();
                Thread.ofVirtual().start(()->{
                    String last=null;long lines=0;
                    try(BufferedReader in=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8))) {
                        for(String line;(line=in.readLine())!=null;) {
                            last=line;
                            if(++lines%1000==0)System.out.println("acked "+line);
                        }
                    } catch(IOException e){/* the writer lost power: expected */}
                    System.out.println("connection closed; LAST ACKED: "+last);
                });
            }
        }
    }

    static void write(Path dir,String host,int port)throws Exception {
        Files.createDirectories(dir);
        String run=UUID.randomUUID().toString().substring(0,8);
        byte[] pad=new byte[512];Arrays.fill(pad,(byte)'x');String padding=new String(pad,StandardCharsets.US_ASCII);
        try(Journal journal=new Journal(dir.resolve("power.wal"),r->{});
            Socket socket=new Socket(host,port);
            Writer out=new OutputStreamWriter(socket.getOutputStream(),StandardCharsets.UTF_8)) {
            socket.setTcpNoDelay(true);
            System.out.println("run "+run+" writing with sync="+journal.stats().get("sync")+"; cut power now");
            for(long seq=1;;seq++) {
                Map<String,Object> record=new LinkedHashMap<>();
                record.put("run",run);record.put("seq",seq);record.put("pad",padding);
                journal.appendBatch(List.of(record));          // returns only once durable
                out.write(run+" "+seq+"\n");out.flush();         // then, and only then, report it
            }
        }
    }

    static boolean verify(Path dir,String run,long acked)throws IOException {
        List<Long> seqs=new ArrayList<>();
        try(Journal ignored=new Journal(dir.resolve("power.wal"),r->{
            if(run.equals(r.get("run")))seqs.add(Json.num(r,"seq",0));
        })){}
        long expect=1;boolean contiguous=true;
        for(long s:seqs){if(s!=expect){contiguous=false;break;}expect++;}
        long max=seqs.isEmpty()?0:seqs.get(seqs.size()-1);
        boolean pass=contiguous&&max>=acked;
        System.out.println((pass?"PASS":"FAIL")+": run "+run+" acknowledged "+acked+", recovered "+seqs.size()
                +" records (max "+max+", contiguous="+contiguous+")"+(pass?"":" -> ACKNOWLEDGED WRITES WERE LOST"));
        return pass;
    }
}
