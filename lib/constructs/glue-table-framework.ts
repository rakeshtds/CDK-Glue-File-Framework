import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import { SchemaLoader } from "../schema-loader/loader";
import { GlueTableConstruct } from "./glue-table-construct";
import { PartitionRefresherConstruct } from "./partition-refresher-construct";
import { ResolvedTableConfig } from "../types";

// ============================================================
// PROPS
// ============================================================
export interface GlueTableFrameworkProps {
  /**
   * Directory containing your schema files (.json / .yaml / .yml).
   * All files in this directory will be loaded and deployed.
   */
  schemaDirectory: string;

  /**
   * Optional: provide a single shared S3 bucket for ALL tables.
   * If not provided, each table creates or references its own bucket.
   */
  sharedBucket?: s3.IBucket;

  /**
   * Optional: override schema directory with explicit schema objects.
   * Useful for programmatic schema generation.
   */
  schemas?: ResolvedTableConfig[];
}

// ============================================================
// FRAMEWORK ORCHESTRATOR
// Usage: new GlueTableFramework(this, 'GlueTables', { schemaDirectory: './schemas' })
// ============================================================
export class GlueTableFramework extends Construct {
  /** Map of tableName → GlueTableConstruct for downstream reference */
  public readonly tables: Map<string, GlueTableConstruct> = new Map();

  /** Map of tableName → PartitionRefresherConstruct */
  public readonly refreshers: Map<string, PartitionRefresherConstruct> = new Map();

  /** All resolved schemas that were deployed */
  public readonly resolvedSchemas: ResolvedTableConfig[];

  constructor(scope: Construct, id: string, props: GlueTableFrameworkProps) {
    super(scope, id);

    // ── Load schemas ────────────────────────────────────────
    this.resolvedSchemas =
      props.schemas ?? SchemaLoader.loadFromDirectory(props.schemaDirectory);

    if (this.resolvedSchemas.length === 0) {
      throw new Error("No schemas loaded — check your schemaDirectory or schemas prop.");
    }

    console.log(
      `[GlueTableFramework] Deploying ${this.resolvedSchemas.length} table(s): ` +
        this.resolvedSchemas.map((s) => `${s.database}.${s.tableName}`).join(", ")
    );

    // ── Deploy each table ───────────────────────────────────
    for (const schema of this.resolvedSchemas) {
      const safeId = `${schema.database}-${schema.tableName}`;

      // 1. Create Glue table + S3 bucket
      const tableConstruct = new GlueTableConstruct(this, `Table-${safeId}`, {
        schema,
        sharedBucket: props.sharedBucket,
      });

      this.tables.set(schema.tableName, tableConstruct);

      // 2. Wire up partition refresh (event / schedule / both)
      const refresher = new PartitionRefresherConstruct(this, `Refresher-${safeId}`, {
        schema,
        bucket: tableConstruct.bucket,
      });

      this.refreshers.set(schema.tableName, refresher);
    }
  }
}
