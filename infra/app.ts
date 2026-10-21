import { App, CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as elb from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secrets from "aws-cdk-lib/aws-secretsmanager";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as path from "node:path";

/**
 * Managed-service deployment: stateless API tasks and SQS workers over DynamoDB (state, history,
 * outbox, idempotency), SQS (dispatch + DLQ), S3 (large payloads) and EventBridge (lifecycle events).
 */
export class ConductorStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const vpc = new ec2.Vpc(this, "Vpc", { maxAzs: 2, natGateways: 1 });
    // Gateway endpoints keep state traffic off the NAT gateway (and its per-GB charge).
    vpc.addGatewayEndpoint("DynamoDbEndpoint", { service: ec2.GatewayVpcEndpointAwsService.DYNAMODB });
    vpc.addGatewayEndpoint("S3Endpoint", { service: ec2.GatewayVpcEndpointAwsService.S3 });

    const table = new dynamodb.TableV2(this, "State", {
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      deletionProtection: true,
      removalPolicy: RemovalPolicy.RETAIN,
      globalSecondaryIndexes: [{
        indexName: "gsi1",
        partitionKey: { name: "gsi1pk", type: dynamodb.AttributeType.STRING },
        sortKey: { name: "gsi1sk", type: dynamodb.AttributeType.STRING },
        projectionType: dynamodb.ProjectionType.INCLUDE,
        nonKeyAttributes: ["id", "status", "name", "createdAt", "updatedAt"],
      }],
    });
    const deadLetters = new sqs.Queue(this, "TaskDeadLetters", {
      queueName: "conductor-tasks-dlq",
      retentionPeriod: Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
    });
    const queue = new sqs.Queue(this, "Tasks", {
      queueName: "conductor-tasks",
      visibilityTimeout: Duration.seconds(30),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      deadLetterQueue: { queue: deadLetters, maxReceiveCount: 5 },
    });
    const payloads = new s3.Bucket(this, "Payloads", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: RemovalPolicy.RETAIN,
      lifecycleRules: [{ transitions: [{ storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: Duration.days(30) }] }],
    });
    const bus = new events.EventBus(this, "Lifecycle", { eventBusName: "conductor" });
    bus.archive("LifecycleArchive", {
      eventPattern: { source: ["dev.conductor"] },
      retention: Duration.days(30),
    });
    const logGroup = new logs.LogGroup(this, "Logs", {
      retention: logs.RetentionDays.ONE_MONTH, removalPolicy: RemovalPolicy.RETAIN,
    });
    const terminalEvents = new logs.LogGroup(this, "TerminalEvents", {
      retention: logs.RetentionDays.ONE_MONTH, removalPolicy: RemovalPolicy.RETAIN,
    });
    new events.Rule(this, "TerminalStatusRule", {
      eventBus: bus,
      eventPattern: {
        source: ["dev.conductor"],
        detailType: ["Execution Status Change"],
        detail: { status: ["SUCCEEDED", "FAILED", "COMPENSATED", "COMPENSATION_FAILED", "CANCELLED"] },
      },
      targets: [new targets.CloudWatchLogGroup(terminalEvents)],
    });

    const token = new secrets.Secret(this, "ApiToken", {
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
    });
    const cluster = new ecs.Cluster(this, "Cluster", { vpc, containerInsightsV2: ecs.ContainerInsights.ENABLED });
    const image = ecs.ContainerImage.fromAsset(path.resolve(__dirname, "../.."));
    const environment = {
      CONDUCTOR_STORE: "dynamodb",
      CONDUCTOR_TABLE: table.tableName,
      CONDUCTOR_QUEUE: queue.queueName,
      CONDUCTOR_BUCKET: payloads.bucketName,
      CONDUCTOR_EVENT_BUS: bus.eventBusName,
      AWS_REGION: this.region,
    };
    const grant = (task: ecs.FargateTaskDefinition) => {
      table.grantReadWriteData(task.taskRole);
      queue.grantSendMessages(task.taskRole);
      queue.grantConsumeMessages(task.taskRole);
      payloads.grantReadWrite(task.taskRole);
      bus.grantPutEventsTo(task.taskRole);
    };

    const apiTask = new ecs.FargateTaskDefinition(this, "ApiTask", { cpu: 512, memoryLimitMiB: 1024 });
    grant(apiTask);
    apiTask.addContainer("Api", {
      image,
      environment: { ...environment, WORKERS: "0", BIND_ADDRESS: "0.0.0.0" },
      secrets: { API_TOKEN: ecs.Secret.fromSecretsManager(token) },
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: "api", logGroup }),
      portMappings: [{ containerPort: 8080 }],
      stopTimeout: Duration.seconds(30),
    });
    // Stateless: rolling deployments with overlap are safe; there is no single writer.
    const api = new ecs.FargateService(this, "Api", {
      cluster, taskDefinition: apiTask, desiredCount: 2,
      minHealthyPercent: 100, maxHealthyPercent: 200, circuitBreaker: { rollback: true },
    });
    const loadBalancer = new elb.ApplicationLoadBalancer(this, "InternalApi", { vpc, internetFacing: false });
    const listener = loadBalancer.addListener("Http", { port: 80, open: false });
    loadBalancer.connections.allowFrom(ec2.Peer.ipv4(vpc.vpcCidrBlock), ec2.Port.tcp(80));
    listener.addTargets("ApiTarget", {
      port: 8080, targets: [api],
      healthCheck: { path: "/healthz" }, deregistrationDelay: Duration.seconds(10),
    });

    const workerTask = new ecs.FargateTaskDefinition(this, "WorkerTask", { cpu: 512, memoryLimitMiB: 1024 });
    grant(workerTask);
    workerTask.addContainer("Worker", {
      image,
      environment: { ...environment, CONDUCTOR_ROLE: "worker", WORKERS: "16" },
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: "worker", logGroup }),
      stopTimeout: Duration.seconds(30),
    });
    const workers = new ecs.FargateService(this, "Workers", {
      cluster, taskDefinition: workerTask, desiredCount: 2,
      minHealthyPercent: 100, maxHealthyPercent: 200, circuitBreaker: { rollback: true },
    });
    const backlog = queue.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(1) });
    workers.autoScaleTaskCount({ minCapacity: 2, maxCapacity: 20 }).scaleOnMetric("QueueBacklog", {
      metric: backlog,
      scalingSteps: [{ upper: 10, change: -1 }, { lower: 100, change: +2 }, { lower: 1000, change: +5 }],
      cooldown: Duration.minutes(2),
    });

    const queueAge = new cloudwatch.Alarm(this, "DispatchDelay", {
      metric: queue.metricApproximateAgeOfOldestMessage({ period: Duration.minutes(1) }),
      threshold: 60, evaluationPeriods: 3,
      alarmDescription: "Ready tasks are waiting over a minute; workers are saturated or failing.",
    });
    const dlq = new cloudwatch.Alarm(this, "DeadLetters", {
      metric: deadLetters.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(1) }),
      threshold: 0, comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD, evaluationPeriods: 1,
      alarmDescription: "Dispatch messages failed five deliveries; inspect worker logs, then redrive.",
    });
    const throttles = new cloudwatch.Alarm(this, "StateThrottles", {
      metric: table.metric("ThrottledRequests", { statistic: "Sum", period: Duration.minutes(1) }),
      threshold: 0, comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD, evaluationPeriods: 3,
      alarmDescription: "DynamoDB is throttling workflow transitions.",
    });
    new cloudwatch.Dashboard(this, "Dashboard", {
      widgets: [
        [
          new cloudwatch.GraphWidget({ title: "Dispatch backlog", left: [backlog], right: [queue.metricApproximateAgeOfOldestMessage()] }),
          new cloudwatch.GraphWidget({ title: "Service CPU", left: [api.metricCpuUtilization(), workers.metricCpuUtilization()] }),
        ],
        [
          new cloudwatch.GraphWidget({
            title: "DynamoDB transaction latency",
            left: [table.metricSuccessfulRequestLatency({ dimensionsMap: { TableName: table.tableName, Operation: "TransactWriteItems" } })],
          }),
          new cloudwatch.AlarmStatusWidget({ title: "Alarms", alarms: [queueAge, dlq, throttles] }),
        ],
      ],
    });
    new CfnOutput(this, "InternalEndpoint", { value: `http://${loadBalancer.loadBalancerDnsName}` });
    new CfnOutput(this, "TokenSecretArn", { value: token.secretArn });
    new CfnOutput(this, "StateTable", { value: table.tableName });
  }
}

if (require.main === module) {
  const app = new App();
  new ConductorStack(app, "Conductor");
}
