package dev.conductor.aws;

import java.util.*;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;

/**
 * Creates the resources that the CDK stack provisions on AWS. Intended for LocalStack and
 * development accounts ({@code conductor.aws.create-resources=true}); production uses CDK.
 */
public final class AwsResources {
    private AwsResources() {}
    public static final String LIST_INDEX="gsi1";

    /** Returns the work queue URL. */
    public static String ensure(AwsClients aws,AwsSettings s) {
        ensureTable(aws,s);
        String dlqUrl=aws.sqs.createQueue(CreateQueueRequest.builder().queueName(s.deadLetterQueue())
                .attributes(Map.of(QueueAttributeName.MESSAGE_RETENTION_PERIOD,"1209600")).build()).queueUrl();
        String dlqArn=aws.sqs.getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(dlqUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN).build()).attributes().get(QueueAttributeName.QUEUE_ARN);
        String queueUrl=aws.sqs.createQueue(CreateQueueRequest.builder().queueName(s.queue()).attributes(Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT,"30",
                QueueAttributeName.REDRIVE_POLICY,"{\"deadLetterTargetArn\":\""+dlqArn+"\",\"maxReceiveCount\":\"5\"}")).build()).queueUrl();
        try{aws.s3.createBucket(CreateBucketRequest.builder().bucket(s.bucket()).build());}
        catch(BucketAlreadyOwnedByYouException|BucketAlreadyExistsException ignored){}
        try{aws.events.createEventBus(CreateEventBusRequest.builder().name(s.eventBus()).build());}
        catch(software.amazon.awssdk.services.eventbridge.model.ResourceAlreadyExistsException ignored){}
        return queueUrl;
    }
    private static void ensureTable(AwsClients aws,AwsSettings s) {
        try {
            aws.dynamo.createTable(CreateTableRequest.builder().tableName(s.table())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(attr("pk"),attr("sk"),attr("gsi1pk"),attr("gsi1sk"))
                    .keySchema(key("pk",KeyType.HASH),key("sk",KeyType.RANGE))
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder().indexName(LIST_INDEX)
                            .keySchema(key("gsi1pk",KeyType.HASH),key("gsi1sk",KeyType.RANGE))
                            .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                    .nonKeyAttributes("id","status","name","createdAt","updatedAt").build()).build())
                    .build());
        } catch(ResourceInUseException ignored) {}
        aws.dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(s.table()).build());
    }
    private static AttributeDefinition attr(String name) {
        return AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build();
    }
    private static KeySchemaElement key(String name,KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
}
