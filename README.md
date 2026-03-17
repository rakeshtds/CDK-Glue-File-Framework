# Glue Table Framework — Finance Data Lake

Schema-driven CDK framework that deploys AWS Glue tables from `.json` / `.yaml` files,
backed by S3, with automatic partition refresh via Lambda + Athena MSCK REPAIR TABLE.

---

## Project structure

```
glue-table-framework/
│
├── bin/
│   └── app.ts                              CDK app entrypoint
│
├── lib/
│   ├── types.ts                            GlueTableSchema TypeScript interface
│   ├── index.ts                            Public exports
│   ├── schema-loader/
│   │   └── loader.ts                       Reads + validates + resolves schema files
│   └── constructs/
│       ├── glue-table-construct.ts         S3 + Glue DB + Glue Table + IAM
│       ├── partition-refresher-construct.ts Lambda + S3 Event + EventBridge
│       └── glue-table-framework.ts         Master orchestrator construct
│
├── schemas/
│   ├── bank_transactions.json              CSV  — daily partitions
│   ├── customer_risk_events.yaml           JSON — hourly partitions
│   └── bank_transactions_avro.json         AVRO — daily partitions
│
├── test-data/
│   ├── README.md                           Upload commands + Athena test queries
│   └── s3-layout/raw/
│       ├── bank_transactions/year=2024/month=03/day=15/
│       ├── customer_risk_events/year=2024/month=03/day=15/hour=12/
│       ├── customer_risk_events/year=2024/month=03/day=15/hour=19/
│       └── bank_transactions_avro/year=2024/month=03/day=15/
│
├── cdk.json
├── package.json
├── tsconfig.json
├── .env.example
└── .gitignore
```

---

## AWS resources deployed (per table)

| Resource | Purpose |
|----------|---------|
| `AWS::S3::Bucket` | Shared data lake bucket (created once) |
| `AWS::Glue::Database` | Glue database (once per db name) |
| `AWS::Glue::Table` | EXTERNAL_TABLE with columns + partitions + SerDe |
| `AWS::Lambda::Function` | Runs MSCK REPAIR TABLE via Athena on trigger |
| S3 Event Notification | Fires Lambda on every file upload |
| `AWS::Events::Rule` | EventBridge cron (if strategy=schedule or both) |
| `AWS::IAM::Role` x2 | Lambda execution role + Glue reader role |

---

## Quick start

```bash
# 1. Install
npm install

# 2. Configure
cp .env.example .env
# Set CDK_DEFAULT_ACCOUNT and CDK_DEFAULT_REGION

# 3. Bootstrap (first time only)
npx cdk bootstrap aws://ACCOUNT_ID/REGION

# 4. Preview
npx cdk synth
npx cdk diff

# 5. Deploy
npx cdk deploy

# 6. Upload test data
BUCKET="data-lake-$(aws sts get-caller-identity --query Account --output text)-us-east-1"
aws s3 cp test-data/s3-layout/raw/ s3://$BUCKET/raw/ --recursive

# 7. Verify in Athena
# SELECT * FROM finance_raw.bank_transactions WHERE year='2024' AND month='03' LIMIT 10;
```

---

## Adding a new table

Drop a `.json` or `.yaml` in `./schemas/` and run `npx cdk deploy`. That is all.

---

## Schema reference

```yaml
database: finance_raw
tableName: my_table
format: CSV                    # CSV | JSON | PARQUET | AVRO | ORC
columns:
  - name: id
    type: string
partitions:
  columns:
    - name: year
      type: string
  s3Pattern: "year={year}"
s3Location:
  prefix: raw/my_table/
refresh:
  strategy: event              # event | schedule | both
  scheduleCron: "0/15 * * * ? *"
```

---

## S3 file naming — critical

```
# CORRECT — Hive partition format
s3://bucket/raw/bank_transactions/year=2024/month=03/day=15/file.csv

# WRONG — MSCK REPAIR will never detect these
s3://bucket/raw/bank_transactions/2024/03/15/file.csv
```
