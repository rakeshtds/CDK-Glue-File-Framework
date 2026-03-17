import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { GlueTableFramework } from "../lib/constructs/glue-table-framework";
import * as path from "path";

// ============================================================
// FINANCE DATA LAKE — CDK STACK
// Deploys Glue tables for:
//   - bank_transactions        (CSV,  daily partitions)
//   - customer_risk_events     (JSON, hourly partitions)
//   - bank_transactions_avro   (AVRO, daily partitions)
//
// Usage:
//   npm install
//   npx cdk synth
//   npx cdk deploy
// ============================================================
export class FinanceDataLakeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ── SHARED S3 DATA LAKE BUCKET ───────────────────────────
    // One bucket for all tables. Each table gets its own prefix:
    //   raw/bank_transactions/year=.../month=.../day=.../
    //   raw/customer_risk_events/year=.../month=.../day=.../hour=.../
    //   raw/bank_transactions_avro/year=.../month=.../day=.../
    const bucketName = process.env.DATA_LAKE_BUCKET_NAME
      ?? `data-lake-${this.account}-${this.region}`;

    const dataLakeBucket = new s3.Bucket(this, "DataLakeBucket", {
      bucketName,
      versioned: false,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          transitions: [
            { storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: cdk.Duration.days(90)  },
            { storageClass: s3.StorageClass.GLACIER,           transitionAfter: cdk.Duration.days(365) },
          ],
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ── GLUE TABLE FRAMEWORK ─────────────────────────────────
    // Reads every .json / .yaml file in ./schemas/
    // Creates one Glue table + partition refresher per file
    const glueTables = new GlueTableFramework(this, "GlueTables", {
      schemaDirectory: path.join(__dirname, "../schemas"),
      sharedBucket: dataLakeBucket,
    });

    // ── STACK OUTPUTS ────────────────────────────────────────
    new cdk.CfnOutput(this, "DataLakeBucketName", {
      value: dataLakeBucket.bucketName,
      description: "Shared S3 bucket — all Glue tables write here",
      exportName: `${id}-DataLakeBucket`,
    });

    new cdk.CfnOutput(this, "TablesDeployed", {
      value: Array.from(glueTables.tables.keys()).join(", "),
      description: "Glue tables created by this stack",
    });

    new cdk.CfnOutput(this, "GlueDatabase", {
      value: "finance_raw",
      description: "Glue database name — use this in Athena queries",
    });

    new cdk.CfnOutput(this, "AthenaQuery", {
      value: "SELECT * FROM finance_raw.bank_transactions WHERE year='2024' AND month='03' LIMIT 10",
      description: "Sample Athena query to verify deployment",
    });
  }
}

// ── CDK APP ENTRYPOINT ───────────────────────────────────────
const app = new cdk.App();

new FinanceDataLakeStack(app, "FinanceDataLakeStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region:  process.env.CDK_DEFAULT_REGION ?? "us-east-1",
  },
  tags: {
    ManagedBy:   "glue-table-framework",
    Environment: process.env.ENVIRONMENT ?? "dev",
    Domain:      "finance",
    CostCenter:  "data-platform",
  },
});
