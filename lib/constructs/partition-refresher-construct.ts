import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import { ResolvedTableConfig } from "../types";

export interface PartitionRefresherProps {
  schema: ResolvedTableConfig;
  bucket: s3.IBucket;
}

// ============================================================
// PARTITION REFRESHER CONSTRUCT
// Supports: "event" | "schedule" | "both"
// ============================================================
export class PartitionRefresherConstruct extends Construct {
  public readonly refreshLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: PartitionRefresherProps) {
    super(scope, id);

    const { schema, bucket } = props;
    const strategy = schema.refresh.strategy;

    // ── 1. IAM ROLE FOR LAMBDA ───────────────────────────────
    const lambdaRole = new iam.Role(this, "RefresherRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
      ],
    });

    // Glue permissions: get/create/delete partitions
    lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "glue:GetTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:GetPartition",
          "glue:CreatePartition",
        ],
        resources: [
          `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
          `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:database/${schema.database}`,
          `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${schema.database}/${schema.tableName}`,
        ],
      })
    );

    // Athena permissions to run MSCK REPAIR TABLE
    lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ],
        resources: ["*"],
      })
    );

    // S3 read permission (needed for Athena query results + listing partitions)
    bucket.grantRead(lambdaRole);
    bucket.grantWrite(lambdaRole, "athena-results/*");

    // ── 2. LAMBDA FUNCTION ───────────────────────────────────
    this.refreshLambda = new lambda.Function(this, "RefresherFn", {
      functionName: `glue-refresh-${schema.database}-${schema.tableName}`,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromInline(this.buildLambdaCode(schema, bucket.bucketName)),
      role: lambdaRole,
      timeout: cdk.Duration.minutes(5),
      memorySize: 256,
      environment: {
        DATABASE: schema.database,
        TABLE: schema.tableName,
        S3_PREFIX: schema.s3Location.prefix,
        BUCKET: bucket.bucketName,
        ATHENA_OUTPUT: `s3://${bucket.bucketName}/athena-results/`,
        HAS_PARTITIONS: schema.partitions ? "true" : "false",
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    // ── 3. S3 EVENT TRIGGER (instant refresh on file upload) ──
    if (strategy === "event" || strategy === "both") {
      bucket.addEventNotification(
        s3.EventType.OBJECT_CREATED,
        new s3n.LambdaDestination(this.refreshLambda),
        { prefix: schema.s3Location.prefix }
      );
    }

    // ── 4. SCHEDULED REFRESH (cron-based) ─────────────────────
    if (strategy === "schedule" || strategy === "both") {
      if (!schema.refresh.scheduleCron) {
        throw new Error(
          `scheduleCron is required for table ${schema.tableName} when strategy is '${strategy}'`
        );
      }

      const rule = new events.Rule(this, "ScheduledRefresh", {
        ruleName: `glue-refresh-schedule-${schema.database}-${schema.tableName}`,
        schedule: events.Schedule.expression(`cron(${schema.refresh.scheduleCron})`),
        description: `Scheduled partition refresh for ${schema.database}.${schema.tableName}`,
      });

      rule.addTarget(
        new targets.LambdaFunction(this.refreshLambda, {
          event: events.RuleTargetInput.fromObject({ source: "scheduled" }),
          retryAttempts: 2,
        })
      );
    }
  }

  // ──────────────────────────────────────────────────────────
  // INLINE LAMBDA CODE
  // Uses Athena to run MSCK REPAIR TABLE — the most reliable
  // way to sync S3 file structure → Glue partition catalog.
  // ──────────────────────────────────────────────────────────
  private buildLambdaCode(schema: ResolvedTableConfig, _bucketName: string): string {
    return `
const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } = require('@aws-sdk/client-athena');

const athena = new AthenaClient({});
const DATABASE = process.env.DATABASE;
const TABLE = process.env.TABLE;
const ATHENA_OUTPUT = process.env.ATHENA_OUTPUT;
const HAS_PARTITIONS = process.env.HAS_PARTITIONS === 'true';

exports.handler = async (event) => {
  console.log('Partition refresh triggered:', JSON.stringify({ source: event.source || 's3-event', database: DATABASE, table: TABLE }));

  if (!HAS_PARTITIONS) {
    console.log('Table has no partitions — nothing to refresh.');
    return { status: 'no-op', reason: 'no partitions defined' };
  }

  const query = \`MSCK REPAIR TABLE \\\`\${DATABASE}\\\`.\\\`\${TABLE}\\\`\`;
  console.log('Running Athena query:', query);

  // Start the query
  const startCmd = new StartQueryExecutionCommand({
    QueryString: query,
    QueryExecutionContext: { Database: DATABASE },
    ResultConfiguration: { OutputLocation: ATHENA_OUTPUT },
    WorkGroup: 'primary',
  });

  const { QueryExecutionId } = await athena.send(startCmd);
  console.log('Query started:', QueryExecutionId);

  // Poll until complete (max ~4 mins)
  const maxAttempts = 48;
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(r => setTimeout(r, 5000)); // 5s between polls

    const statusCmd = new GetQueryExecutionCommand({ QueryExecutionId });
    const { QueryExecution } = await athena.send(statusCmd);
    const state = QueryExecution?.Status?.State;

    console.log(\`Poll \${i + 1}: \${state}\`);

    if (state === 'SUCCEEDED') {
      console.log('Partition refresh SUCCEEDED');
      return { status: 'success', queryId: QueryExecutionId };
    }
    if (state === 'FAILED' || state === 'CANCELLED') {
      const reason = QueryExecution?.Status?.StateChangeReason;
      console.error('Query failed:', reason);
      throw new Error(\`MSCK REPAIR TABLE failed: \${reason}\`);
    }
  }

  throw new Error('Timed out waiting for MSCK REPAIR TABLE to complete');
};
`;
  }
}
