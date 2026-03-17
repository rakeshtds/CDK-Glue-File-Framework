import * as fs from "fs";
import * as path from "path";
import * as yaml from "js-yaml";
import { GlueTableSchema, ResolvedTableConfig, SerdeConfig, TableFormat } from "../types";

// ============================================================
// DEFAULT SERDE CONFIGS PER FORMAT
// ============================================================
const DEFAULT_SERDES: Record<TableFormat, SerdeConfig> = {
  PARQUET: {
    serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    parameters: { "serialization.format": "1" },
  },
  CSV: {
    serializationLibrary: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    parameters: {
      "field.delim": ",",
      "line.delim": "\n",
      "serialization.format": ",",
    },
  },
  JSON: {
    serializationLibrary: "org.openx.data.jsonserde.JsonSerDe",
    parameters: { "ignore.malformed.json": "TRUE" },
  },
  ORC: {
    serializationLibrary: "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    parameters: {},
  },
  AVRO: {
    serializationLibrary: "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
    parameters: {},
  },
};

// ============================================================
// SCHEMA LOADER
// ============================================================
export class SchemaLoader {
  /**
   * Load all schema files from a directory.
   * Supports both .json and .yaml / .yml files.
   */
  static loadFromDirectory(schemaDir: string): ResolvedTableConfig[] {
    const absoluteDir = path.resolve(schemaDir);

    if (!fs.existsSync(absoluteDir)) {
      throw new Error(`Schema directory not found: ${absoluteDir}`);
    }

    const files = fs
      .readdirSync(absoluteDir)
      .filter((f) => /\.(json|ya?ml)$/i.test(f));

    if (files.length === 0) {
      throw new Error(`No schema files (.json / .yaml / .yml) found in: ${absoluteDir}`);
    }

    return files.map((file) => {
      const filePath = path.join(absoluteDir, file);
      const raw = SchemaLoader.loadFile(filePath);
      SchemaLoader.validate(raw, filePath);
      return SchemaLoader.resolve(raw);
    });
  }

  /** Load a single schema file by path */
  static loadOne(filePath: string): ResolvedTableConfig {
    const raw = SchemaLoader.loadFile(filePath);
    SchemaLoader.validate(raw, filePath);
    return SchemaLoader.resolve(raw);
  }

  // ----------------------------------------------------------
  // PRIVATE HELPERS
  // ----------------------------------------------------------

  private static loadFile(filePath: string): GlueTableSchema {
    const content = fs.readFileSync(filePath, "utf-8");
    const ext = path.extname(filePath).toLowerCase();

    try {
      if (ext === ".json") {
        return JSON.parse(content) as GlueTableSchema;
      } else {
        return yaml.load(content) as GlueTableSchema;
      }
    } catch (err) {
      throw new Error(`Failed to parse schema file ${filePath}: ${(err as Error).message}`);
    }
  }

  private static validate(schema: GlueTableSchema, filePath: string): void {
    const errors: string[] = [];

    if (!schema.database) errors.push("'database' is required");
    if (!schema.tableName) errors.push("'tableName' is required");
    if (!schema.format) errors.push("'format' is required");
    if (!schema.columns || schema.columns.length === 0)
      errors.push("'columns' must have at least one entry");
    if (!schema.s3Location?.prefix)
      errors.push("'s3Location.prefix' is required");
    if (!schema.refresh?.strategy)
      errors.push("'refresh.strategy' is required (event | schedule | both)");
    if (
      (schema.refresh?.strategy === "schedule" || schema.refresh?.strategy === "both") &&
      !schema.refresh?.scheduleCron
    ) {
      errors.push("'refresh.scheduleCron' is required when strategy is 'schedule' or 'both'");
    }

    // Validate column names are unique
    const colNames = schema.columns.map((c) => c.name);
    const dupes = colNames.filter((n, i) => colNames.indexOf(n) !== i);
    if (dupes.length > 0) errors.push(`Duplicate column names: ${dupes.join(", ")}`);

    if (errors.length > 0) {
      throw new Error(
        `Schema validation failed for ${filePath}:\n  - ${errors.join("\n  - ")}`
      );
    }
  }

  private static resolve(schema: GlueTableSchema): ResolvedTableConfig {
    const bucket = schema.s3Location.bucketName ?? `<<managed-bucket-for-${schema.database}>>`;
    const prefix = schema.s3Location.prefix.replace(/\/?$/, "/"); // ensure trailing slash
    const s3Uri = `s3://${bucket}/${prefix}`;
    const serde = schema.serdeOverride ?? DEFAULT_SERDES[schema.format];

    return { ...schema, s3Uri, serde, s3Location: { ...schema.s3Location, prefix } };
  }
}
