package dev.conductor.aws;

import dev.conductor.core.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * Keeps large task results out of the 400 KB DynamoDB item. Objects are content-addressed by
 * SHA-256 and never overwritten with different bytes, so a pinned execution's reference stays valid.
 * The object is written before the reference is committed; an aborted transaction leaves at most
 * an unreferenced object for a lifecycle rule to expire.
 */
public final class PayloadStore {
    public static final String REF="$payloadRef";
    private final S3Client s3;
    private final String bucket;
    private final int threshold;
    public PayloadStore(S3Client s3,String bucket,int threshold){this.s3=s3;this.bucket=bucket;this.threshold=threshold;}

    /** Replaces oversized task results in {@code run} with S3 references. */
    public void offload(Map<String,Object> run) {
        for(Object value:Workflow.tasks(run).values()) {
            Map<String,Object> task=Json.obj(value);
            Object result=task.get("result");
            if(result==null||isRef(result))continue;
            byte[] bytes=Json.write(result).getBytes(StandardCharsets.UTF_8);
            if(bytes.length<=threshold)continue;
            String hash=sha256(bytes),key="payloads/"+hash+".json";
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).contentType("application/json").build(),RequestBody.fromBytes(bytes));
            Map<String,Object> ref=new LinkedHashMap<>();
            ref.put("bucket",bucket);ref.put("key",key);ref.put("sha256",hash);ref.put("bytes",(long)bytes.length);
            task.put("result",Map.of(REF,ref));
        }
    }
    /** Inlines referenced results for API readers, verifying the content hash. */
    public void resolve(Map<String,Object> run) {
        for(Object value:Workflow.tasks(run).values()) {
            Map<String,Object> task=Json.obj(value);
            Object result=task.get("result");
            if(!isRef(result))continue;
            Map<String,Object> ref=Json.obj(Json.obj(result).get(REF));
            byte[] bytes=s3.getObjectAsBytes(GetObjectRequest.builder().bucket(Json.str(ref,"bucket",bucket)).key(Json.str(ref,"key","")).build()).asByteArray();
            if(!sha256(bytes).equals(Json.str(ref,"sha256","")))throw new IllegalStateException("Payload checksum mismatch for "+ref.get("key"));
            task.put("result",Json.parse(new String(bytes,StandardCharsets.UTF_8)));
            task.put("resultStoredIn","s3");
        }
    }
    static boolean isRef(Object value){return value instanceof Map<?,?> m&&m.size()==1&&m.containsKey(REF);}
    static String sha256(byte[] bytes) {
        try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}
        catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}
    }
}
