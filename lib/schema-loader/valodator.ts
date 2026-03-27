/**
 * validator.ts
 * -----------
 * Standalone CLI tool to validate all schema files in the schemas/ directory
 * before running cdk deploy.
 *
 * Usage:
 *   npm run validate-schemas
 *   ts-node lib/schema-loader/validator.ts
 *   ts-node lib/schema-loader/validator.ts ./schemas/bank_transactions.json
 */

import * as path from "path";
import * as fs from "fs";
import { SchemaLoader } from "./loader";

// ── COLOUR HELPERS (no external deps) ───────────────────────
const green  = (s: string) => `\x1b[32m${s}\x1b[0m`;
const red    = (s: string) => `\x1b[31m${s}\x1b[0m`;
const yellow = (s: string) => `\x1b[33m${s}\x1b[0m`;
const bold   = (s: string) => `\x1b[1m${s}\x1b[0m`;
const dim    = (s: string) => `\x1b[2m${s}\x1b[0m`;

// ── ENTRY POINT ──────────────────────────────────────────────
function main() {
  const args = process.argv.slice(2);

  // If a specific file path was passed, validate just that file
  if (args.length > 0 && fs.existsSync(args[0])) {
    validateSingleFile(args[0]);
  } else {
    validateDirectory();
  }
}

// ── VALIDATE ALL FILES IN schemas/ ──────────────────────────
function validateDirectory() {
  const schemaDir = path.resolve(__dirname, "../../schemas");

  console.log(bold("\n Glue Table Framework — Schema Validator"));
  console.log(dim(`  Directory: ${schemaDir}\n`));

  if (!fs.existsSync(schemaDir)) {
    console.error(red(`  ERROR: schemas/ directory not found at ${schemaDir}`));
    process.exit(1);
  }

  const files = fs.readdirSync(schemaDir).filter(f => /\.(json|ya?ml)$/i.test(f));

  if (files.length === 0) {
    console.warn(yellow("  WARNING: No .json or .yaml files found in schemas/"));
    process.exit(0);
  }

  let passed = 0;
  let failed = 0;

  for (const file of files) {
    const filePath = path.join(schemaDir, file);
    const result = validateFile(filePath);
    if (result.ok) passed++;
    else failed++;
  }

  // ── SUMMARY ─────────────────────────────────────────────
  console.log("\n" + "─".repeat(52));
  console.log(bold(`  Results: ${files.length} file(s) checked`));
  if (passed > 0) console.log(green(`  ${passed} passed`));
  if (failed > 0) console.log(red(`  ${failed} failed`));
  console.log("─".repeat(52) + "\n");

  if (failed > 0) {
    console.error(red("  Fix the errors above before running cdk deploy.\n"));
    process.exit(1);
  } else {
    console.log(green("  All schemas valid. Safe to run: npx cdk deploy\n"));
    process.exit(0);
  }
}

// ── VALIDATE A SINGLE FILE ───────────────────────────────────
function validateSingleFile(filePath: string) {
  console.log(bold("\n Glue Table Framework — Schema Validator"));
  console.log(dim(`  File: ${filePath}\n`));
  const result = validateFile(filePath);
  console.log();
  if (!result.ok) process.exit(1);
}

// ── CORE VALIDATE LOGIC (returns ok/error) ───────────────────
function validateFile(filePath: string): { ok: boolean } {
  const fileName = path.basename(filePath);

  try {
    const resolved = SchemaLoader.loadOne(filePath);

    // Extra warnings (non-fatal)
    const warnings: string[] = [];

    if (!resolved.description) {
      warnings.push("no 'description' field — consider adding one for the Glue catalog");
    }
    if (!resolved.tags || Object.keys(resolved.tags).length === 0) {
      warnings.push("no 'tags' defined — recommended for cost tracking");
    }
    if (resolved.columns.length > 50) {
      warnings.push(`${resolved.columns.length} columns — very wide table, consider splitting`);
    }
    if (
      (resolved.refresh.strategy === "schedule" || resolved.refresh.strategy === "both") &&
      resolved.refresh.scheduleCron
    ) {
      const cronFields = resolved.refresh.scheduleCron.trim().split(/\s+/);
      if (cronFields.length !== 6) {
        warnings.push(
          `scheduleCron has ${cronFields.length} fields — AWS EventBridge requires exactly 6 (min hr dom mon dow year)`
        );
      }
    }

    // Print result
    console.log(green(`  PASS`) + `  ${fileName}`);
    console.log(dim(`        → ${resolved.database}.${resolved.tableName}`));
    console.log(dim(`        → format: ${resolved.format}  |  columns: ${resolved.columns.length}  |  partitions: ${resolved.partitions?.columns.length ?? 0}  |  refresh: ${resolved.refresh.strategy}`));
    console.log(dim(`        → s3: ${resolved.s3Uri}`));

    if (warnings.length > 0) {
      warnings.forEach(w => console.log(yellow(`  WARN    ${w}`)));
    }

    console.log();
    return { ok: true };

  } catch (err) {
    console.log(red(`  FAIL`) + `  ${fileName}`);
    const message = (err as Error).message;
    message.split("\n").forEach(line => console.log(red(`        ${line}`)));
    console.log();
    return { ok: false };
  }
}

main();
