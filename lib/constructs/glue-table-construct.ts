import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as glue from "aws-cdk-lib/aws-glue";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import { ResolvedTableConfig, TableFormat } from "../types";

// ============================================================
// INPUT/OUTPUT FORMATS PER FILE FORMAT
// ============================================================
interface FormatDescriptor {
  inputFormat: string;
  outputFormat: string;
}

const FORMAT_DESCRIPTORS: Record<TableFormat, FormatDescriptor> = {
  PARQUET: {
    inputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
  },
  CSV: {
    inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
  },
  JSON: {
    inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
  },
  ORC: {
    inputFormat: "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
  },
  AVRO: {
    inputFormat: "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
  },
};

// ============================================================
// PROPS
// ============================================================
export interface GlueTableConstructProps {
  schema: ResolvedTableConfig;
  /**
   * Pass an existing bucket if you want all tables to share one bucket.
   * If undefined AND schema.s3Location.bucketName is undefined, a new bucket is created.
   */
  sharedBucket?: s3.IBucket;
}

// ============================================================
// CONSTRUCT
// ============================================================
export class GlueTableConstruct extends Construct {
  /** The S3 bucket backing this table */
  public readonly bucket: s3.IBucket;
  /** The Glue table resource */
  public readonly table: glue.CfnTable;
  /** IAM role that can read this table's data */
  public readonly readerRole: iam.Role;

  constructor(scope: Construct, id: string, props: GlueTableConstructProps) {
    super(scope, id);

    const { schema } = props;

    // ── 1. S3 BUCKET ────────────────────────────────────────
    if (props.sharedBucket) {
      this.bucket = props.sharedBucket;
    } else if (schema.s3Location.bucketName) {
      this.bucket = s3.Bucket.fromBucketName(
        this,
        "ExistingBucket",
        schema.s3Location.bucketName
      );
    } else {
      // Auto-create a purpose-built bucket for this database
      this.bucket = new s3.Bucket(this, "DataBucket", {
        bucketName: `${schema.database}-${schema.tableName}-${cdk.Stack.of(this).account}`,
        versioned: false,
        encryption: s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        lifecycleRules: [
          {
            // Move old data to IA after 90 days, Glacier after 365
            transitions: [
              { storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: cdk.Duration.days(90) },
              { storageClass: s3.StorageClass.GLACIER, transitionAfter: cdk.Duration.days(365) },
            ],
          },
        ],
        removalPolicy: cdk.RemovalPolicy.RETAIN, // Never auto-delete data!
      });

      // Tag the bucket
      if (schema.tags) {
        Object.entries(schema.tags).forEach(([k, v]) =>
          cdk.Tags.of(this.bucket).add(k, v)
        );
      }
    }

    // ── 2. GLUE DATABASE (idempotent — reuse if it already exists) ──
    const dbId = `GlueDb-${schema.database}`;
    const existingDb = scope.node.tryFindChild(dbId);
    if (!existingDb) {
      new glue.CfnDatabase(scope, dbId, {
        catalogId: cdk.Stack.of(this).account,
        databaseInput: {
          name: schema.database,
          description: `Managed by glue-table-framework`,
        },
      });
    }

    // ── 3. BUILD COLUMN DEFINITIONS ─────────────────────────
    const toGlueCols = (cols: ResolvedTableConfig["columns"]) =>
      cols.map((c) => ({
        name: c.name,
        type: c.type,
        comment: c.comment ?? "",
      }));

    const formatDesc = FORMAT_DESCRIPTORS[schema.format];

    // ── 4. GLUE TABLE ────────────────────────────────────────
    this.table = new glue.CfnTable(this, "GlueTable", {
      catalogId: cdk.Stack.of(this).account,
      databaseName: schema.database,
      tableInput: {
        name: schema.tableName,
        description: schema.description ?? "",
        tableType: "EXTERNAL_TABLE",
        parameters: {
          "classification": schema.format.toLowerCase(),
          "EXTERNAL": "TRUE",
          "projection.enabled": "false", // Enable partition projection later if needed
        },
        storageDescriptor: {
          location: schema.s3Uri,
          columns: toGlueCols(schema.columns),
          inputFormat: formatDesc.inputFormat,
          outputFormat: formatDesc.outputFormat,
          compressed: schema.format === "PARQUET" || schema.format === "ORC",
          numberOfBuckets: -1,
          serdeInfo: {
            serializationLibrary: schema.serde.serializationLibrary,
            parameters: schema.serde.parameters ?? {},
          },
          storedAsSubDirectories: false,
        },
        partitionKeys: schema.partitions ? toGlueCols(schema.partitions.columns) : [],
      },
    });

    // ── 5. READER IAM ROLE ───────────────────────────────────
    this.readerRole = new iam.Role(this, "ReaderRole", {
      roleName: `glue-reader-${schema.database}-${schema.tableName}`,
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      description: `Read access for Glue table ${schema.database}.${schema.tableName}`,
    });

    this.bucket.grantRead(this.readerRole, `${schema.s3Location.prefix}*`);

    this.readerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["glue:GetTable", "glue:GetPartitions", "glue:GetDatabase"],
        resources: [
          `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
          `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:database/${schema.database}`,
          `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${schema.database}/${schema.tableName}`,
        ],
      })
    );

    // ── 6. TAGS ──────────────────────────────────────────────
    if (schema.tags) {
      cdk.Tags.of(this).add("GlueDatabase", schema.database);
      cdk.Tags.of(this).add("GlueTable", schema.tableName);
      Object.entries(schema.tags).forEach(([k, v]) => cdk.Tags.of(this).add(k, v));
    }
  }
}
