package dev.conductor.aws;

import java.net.URI;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/** One shared connection pool for all four services; closed with the backend. */
public final class AwsClients implements AutoCloseable {
    public final DynamoDbClient dynamo;
    public final SqsClient sqs;
    public final S3Client s3;
    public final EventBridgeClient events;
    private final SdkHttpClient http;
    private final DefaultCredentialsProvider credentials;

    public AwsClients(AwsSettings settings) {
        http=Apache5HttpClient.builder().maxConnections(256).connectionTimeout(Duration.ofSeconds(3))
                .socketTimeout(Duration.ofSeconds(30)).build();
        credentials=DefaultCredentialsProvider.builder().build();
        dynamo=configure(DynamoDbClient.builder(),settings).build();
        sqs=configure(SqsClient.builder(),settings).build();
        s3=configure(S3Client.builder().forcePathStyle(settings.endpoint()!=null&&!settings.endpoint().isBlank()),settings).build();
        events=configure(EventBridgeClient.builder(),settings).build();
    }
    private <B extends AwsClientBuilder<B,?>> B configure(B builder,AwsSettings settings) {
        builder.region(Region.of(settings.region())).credentialsProvider(credentials)
                .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofSeconds(30)).build());
        if(builder instanceof software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder<?,?> sync)sync.httpClient(http);
        if(settings.endpoint()!=null&&!settings.endpoint().isBlank())builder.endpointOverride(URI.create(settings.endpoint()));
        return builder;
    }
    @Override public void close() {
        dynamo.close();sqs.close();s3.close();events.close();http.close();credentials.close();
    }
}
