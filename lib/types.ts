// ============================================================
// GLUE TABLE FRAMEWORK — Core Type Definitions
// All schema files you provide must conform to GlueTableSchema
// ============================================================

export type GlueColumnType =
  | "string"
  | "int"
  | "bigint"
  | "double"
  | "float"
  | "boolean"
  | "timestamp"
  | "date"
  | "binary"
  | `array<${string}>`
  | `map<${string},${string}>`
  | `struct<${string}>`;

export interface GlueColumn {
  /** Column name (snake_case recommended) */
  name: string;
  /** Glue-compatible data type */
  type: GlueColumnType | string;
  /** Human-readable description for the Glue Data Catalog */
  comment?: string;
}

export interface PartitionConfig {
  /** Columns used to partition data on S3 (e.g. year, month, day) */
  columns: GlueColumn[];
  /**
   * S3 key prefix pattern using {column} placeholders.
   * Example: "year={year}/month={month}/day={day}"
   */
  s3Pattern: string;
}

export interface S3LocationConfig {
  /**
   * S3 bucket name. Leave empty to auto-create a managed bucket.
   * If provided, the bucket must already exist.
   */
  bucketName?: string;
  /**
   * Prefix/folder within the bucket for this table.
   * Example: "raw/orders/"
   */
  prefix: string;
}

export interface SerdeConfig {
  /** Serialization library class name */
  serializationLibrary: string;
  /** Additional SerDe parameters */
  parameters?: Record<string, string>;
}

export type TableFormat = "PARQUET" | "CSV" | "JSON" | "ORC" | "AVRO";

export interface RefreshConfig {
  /**
   * "event"    — S3 Event Notification triggers a Lambda that runs MSCK REPAIR TABLE instantly.
   * "schedule" — EventBridge rule runs MSCK REPAIR TABLE on a cron schedule.
   * "both"     — Both mechanisms are deployed.
   */
  strategy: "event" | "schedule" | "both";
  /**
   * Cron expression for scheduled refresh (required if strategy is "schedule" or "both").
   * Example: "0 * * * ? *"  (every hour)
   */
  scheduleCron?: string;
}

// -------------------------------------------------------
// TOP-LEVEL SCHEMA — this is what you fill in per table
// -------------------------------------------------------
export interface GlueTableSchema {
  /** Logical database name in the Glue Data Catalog */
  database: string;
  /** Table name in the Glue Data Catalog */
  tableName: string;
  /** Human-readable description shown in the catalog */
  description?: string;
  /** File format of incoming data on S3 */
  format: TableFormat;
  /** Regular (non-partition) columns */
  columns: GlueColumn[];
  /** Optional partitioning configuration */
  partitions?: PartitionConfig;
  /** S3 location configuration for this table */
  s3Location: S3LocationConfig;
  /**
   * Custom SerDe override.
   * If omitted the framework picks sensible defaults per format.
   */
  serdeOverride?: SerdeConfig;
  /** How to keep Glue partitions in sync when new files land */
  refresh: RefreshConfig;
  /** Arbitrary tags applied to all resources created for this table */
  tags?: Record<string, string>;
}

// -------------------------------------------------------
// RESOLVED CONFIG — used internally after loading schemas
// -------------------------------------------------------
export interface ResolvedTableConfig extends GlueTableSchema {
  /** Fully-qualified S3 URI: s3://bucket/prefix/ */
  s3Uri: string;
  /** Resolved SerDe (never undefined after resolution) */
  serde: SerdeConfig;
}
