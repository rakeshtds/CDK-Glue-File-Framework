# Test Data — Finance Pipeline

Dummy data for testing all three table formats.
All records are fictional. Realistic finance domain values for end-to-end pipeline testing.

---

## Files generated

| File | Format | Table | Records |
|------|--------|-------|---------|
| `s3-layout/raw/bank_transactions/year=2024/month=03/day=15/bank_transactions_part0001.csv` | CSV | bank_transactions | 20 |
| `s3-layout/raw/bank_transactions/year=2024/month=03/day=16/bank_transactions_part0001.csv` | CSV | bank_transactions | 1  |
| `s3-layout/raw/bank_transactions_avro/year=2024/month=03/day=15/bank_transactions_part0001.avro` | AVRO | bank_transactions_avro | 12 |
| `s3-layout/raw/customer_risk_events/year=2024/month=03/day=15/hour=12/events_part0001.json` | JSON (NDJSON) | customer_risk_events | 5  |
| `s3-layout/raw/customer_risk_events/year=2024/month=03/day=15/hour=19/events_part0001.json` | JSON (NDJSON) | customer_risk_events | 5  |

---

## What the data covers (finance scenarios)

### CSV — bank_transactions
- Normal debit/credit transactions (POS, ATM, ONLINE, MOBILE, BRANCH)
- Salary deposits, mortgage payments, wire transfers
- Flagged transactions: large cash deposit ($50k), large wire ($15k), cashier cheque ($10k)
- One FAILED transaction (ATM), one REVERSAL (dispute), one PENDING
- Covers MCC codes: 5411 (grocery), 5812 (restaurants), 5541 (gas), 7372 (streaming)

### JSON — customer_risk_events (NDJSON — one JSON object per line)
- LARGE_TRANSFER: wire transfer flagged by rules R001+R045+R089, risk_score=0.87
- LOGIN_ANOMALY: login from Russia on new device, risk_score=0.62
- AML_ALERT: $50k cash deposit, CRITICAL band, case created (CASE-2024-00441)
- KYC_CHANGE: address change, LOW risk, no action
- SANCTION_HIT: OFAC match, account frozen, CRITICAL (risk_score=0.99)
- DEVICE_CHANGE: new iPhone, same network, LOW risk
- Includes resolved events (resolution_ts set) and open cases (null)

### AVRO — bank_transactions_avro
- Same transaction schema as CSV but in binary AVRO format
- Includes embedded Avro schema (namespace: finance_raw)
- Partition columns (year/month/day) embedded as fields in the record

---

## S3 upload commands

```bash
# Set your bucket name
BUCKET="my-finance-data-lake"

# Upload CSV (bank_transactions — daily partition)
aws s3 cp "s3-layout/raw/bank_transactions/" \
  "s3://$BUCKET/raw/bank_transactions/" --recursive

# Upload JSON (customer_risk_events — hourly partition)
aws s3 cp "s3-layout/raw/customer_risk_events/" \
  "s3://$BUCKET/raw/customer_risk_events/" --recursive

# Upload AVRO (bank_transactions_avro)
aws s3 cp "s3-layout/raw/bank_transactions_avro/" \
  "s3://$BUCKET/raw/bank_transactions_avro/" --recursive
```

After upload, MSCK REPAIR TABLE fires automatically via Lambda.

---

## Verify in Athena after upload

```sql
-- Check partitions registered
SHOW PARTITIONS finance_raw.bank_transactions;
SHOW PARTITIONS finance_raw.customer_risk_events;

-- Query CSV table
SELECT transaction_type, COUNT(*) as cnt, SUM(amount) as total
FROM finance_raw.bank_transactions
WHERE year='2024' AND month='03' AND day='15'
GROUP BY transaction_type;

-- Find flagged transactions
SELECT transaction_id, amount, channel, merchant_name
FROM finance_raw.bank_transactions
WHERE year='2024' AND month='03' AND day='15'
  AND is_flagged = true;

-- Query JSON risk events — critical only
SELECT event_id, event_type, customer_id, risk_score, action_taken
FROM finance_raw.customer_risk_events
WHERE year='2024' AND month='03' AND day='15'
  AND risk_band = 'CRITICAL';

-- Unnest triggered_rules array
SELECT event_id, rule
FROM finance_raw.customer_risk_events
CROSS JOIN UNNEST(triggered_rules) AS t(rule)
WHERE year='2024' AND month='03' AND day='15';
```

---

## CSV notes
- Has header row — matches `skip.header.line.count: "1"` in schema SerDe
- Delimiter: comma
- Null values: empty string (e.g. merchant_code left blank for non-POS transactions)

## JSON notes
- NDJSON format (newline-delimited JSON) — one complete JSON object per line
- NOT a JSON array — this is the correct format for Glue/Athena
- `ignore.malformed.json: TRUE` in SerDe will skip any corrupt lines gracefully

## AVRO notes
- Binary format — not human-readable directly
- Schema embedded in file header — Glue reads it automatically
- Partition columns (year/month/day) are included as regular fields in the AVRO schema
