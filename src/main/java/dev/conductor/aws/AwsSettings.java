package dev.conductor.aws;

/**
 * Names of the managed resources. {@code endpoint} is set only for emulators such as LocalStack;
 * on AWS the SDK resolves regional endpoints and credentials from the task role.
 */
public record AwsSettings(String region,String endpoint,String table,String queue,String bucket,String eventBus,
                          boolean createResources,int payloadThresholdBytes) {
    public AwsSettings {
        if(table==null||table.isBlank()||queue==null||queue.isBlank()||bucket==null||bucket.isBlank()||eventBus==null||eventBus.isBlank())
            throw new IllegalArgumentException("dynamodb store requires table, queue, bucket and event bus names");
        if(payloadThresholdBytes<1024||payloadThresholdBytes>200_000)
            throw new IllegalArgumentException("payload threshold must be 1 KiB..200 KB");
    }
    public String deadLetterQueue(){return queue+"-dlq";}
}
