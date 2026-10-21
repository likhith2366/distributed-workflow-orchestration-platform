import { App } from "aws-cdk-lib";
import { Match, Template } from "aws-cdk-lib/assertions";
import { ConductorStack } from "./app";

const template = Template.fromStack(new ConductorStack(new App(), "Test"));

template.hasResourceProperties("AWS::DynamoDB::GlobalTable", {
  KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
  BillingMode: "PAY_PER_REQUEST",
  GlobalSecondaryIndexes: [Match.objectLike({ IndexName: "gsi1" })],
  Replicas: [Match.objectLike({ PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true }, DeletionProtectionEnabled: true })],
});
template.hasResourceProperties("AWS::SQS::Queue", {
  QueueName: "conductor-tasks",
  RedrivePolicy: Match.objectLike({ maxReceiveCount: 5 }),
  SqsManagedSseEnabled: true,
});
template.hasResourceProperties("AWS::S3::Bucket", {
  PublicAccessBlockConfiguration: { BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true },
});
template.hasResourceProperties("AWS::Events::EventBus", { Name: "conductor" });
template.resourceCountIs("AWS::Events::Archive", 1);
template.hasResourceProperties("AWS::Events::Rule", {
  EventPattern: Match.objectLike({ source: ["dev.conductor"] }),
});
template.hasResourceProperties("AWS::ElasticLoadBalancingV2::LoadBalancer", { Scheme: "internal" });
template.resourceCountIs("AWS::ECS::Service", 2);
template.hasResourceProperties("AWS::ECS::Service", { DesiredCount: 2 });
template.resourceCountIs("AWS::ApplicationAutoScaling::ScalableTarget", 1);
template.resourceCountIs("AWS::EC2::VPCEndpoint", 2);
template.resourceCountIs("AWS::EFS::FileSystem", 0);
template.hasResourceProperties("AWS::ECS::TaskDefinition", {
  ContainerDefinitions: [Match.objectLike({
    Environment: Match.arrayWith([{ Name: "CONDUCTOR_STORE", Value: "dynamodb" }, { Name: "CONDUCTOR_ROLE", Value: "worker" }]),
  })],
});
// Workers hold no API token: they never call the HTTP API.
template.hasResourceProperties("AWS::ECS::TaskDefinition", {
  ContainerDefinitions: [Match.objectLike({ Name: "Worker", Secrets: Match.absent() })],
});
// Three operational alarms plus the two that drive step scaling on queue backlog.
template.resourceCountIs("AWS::CloudWatch::Alarm", 5);
console.log("PASS infrastructure: DynamoDB state + PITR, SQS with DLQ, private S3, EventBridge bus/archive/rule, stateless API x2, autoscaled workers, alarms");
