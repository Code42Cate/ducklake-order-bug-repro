# DuckLake Bug: ORDER BY + LIMIT ignores WHERE on inlined data

When data is inlined (stored in PostgreSQL, not yet flushed to S3), queries with `ORDER BY ... LIMIT` return rows that violate the `WHERE` clause. After flushing to S3, the same query returns correct results.

## Expected

```
SELECT id, category FROM lake.test_data WHERE category = 'A' ORDER BY created_at DESC LIMIT 3
```

Should return only rows where `category = 'A'`.

## Actual

Before flush (inlined data):
```
a_0 A [ok]
b_0 B [BUG]   <-- violates WHERE category = 'A'
a_1 A [ok]
```

After flush (data in S3):
```
a_0 A [ok]
a_1 A [ok]
a_2 A [ok]
```

## Reproduce

```bash
docker compose up repro
```

## Setup

- DuckDB with DuckLake extension (1.4.3)
- PostgreSQL 16
- S3-compatible storage (rustfs)
- `DATA_INLINING_ROW_LIMIT 100` to ensure data is inlined before flush
- Table with 6 rows: 3 with category A, 3 with category B
- Same timestamps for A/B pairs to expose ordering issue

