# Design decisions, trade-offs and failure modes

## 1. Star schema with deterministic surrogate keys
Surrogate keys are `xxhash64(business_key)`. They are stable across reruns, need no lookup table, and parallelise
perfectly, so the load stays idempotent. Trade-off: hash collisions are theoretically possible (negligible for 64-bit
keys at this scale; a uniqueness check is part of the tests) and keys are not human-readable.

## 2. SCD Type 1 dimensions (and its cost)
Dealerships and products are overwritten in place. It is simple and matches how the sample feed behaves, but history
is lost: when a target or unit cost changes, past months are re-evaluated with the **current** value. The README and
the mart SQL say so explicitly. The production fix is SCD Type 2 (effective-from/to, current flag) with the fact joining
on the version valid at the sale date.

## 3. Referential integrity at Silver, not Gold
A sale whose dealership or product is unknown is quarantined as `orphan_*`. This keeps every Gold fact joinable to its
dimensions (asserted in the integration test). Known limitation: if the missing reference row arrives *later*, the
quarantined sale is not replayed automatically. A replay job reading the rejects table is a listed future enhancement.
Reference files are processed first in every run so same-batch references work.

## 4. Reject vs impute
- A missing or invalid value in something the KPIs depend on (date, quantity, price, ids, payment type) rejects the row.
- A missing **discount** is imputed as 0 and a missing **customer type** as `Unknown`, with a warning recorded. Imputing a
  zero discount can overstate revenue slightly if the discount was really missing, which is why it is flagged and counted.
- Garbage in an optional field (for example `discount_pct = "lots"`) is rejected, not imputed: "missing" and "wrong" are different failures.

## 5. Corrections, duplicates and late data
The business key is `sale_id`. Same key and same values is a duplicate; same key and different values is a correction (latest
ingestion wins, and an older ingestion can never overwrite a newer one). A new key older than that dealership's Silver
maximum date is a late arrival: accepted, flagged, and the affected months are recomputed.

## 6. Inventory reconciliation as a data-quality control
`opening + received - sold - closing = 0` per dealership, product and week. It is the cheapest independent cross-check between
two feeds. In the sample, after batch 1 it flags exactly the 12 cells touched by the 12 withheld late sales; after batch 2 it is 0.
A non-zero value means a feed is incomplete or wrong, not that the pipeline is wrong. The first snapshot per series has no opening
balance, so its difference is NULL by design.

## 7. Incremental strategy for Gold
Dimensions are tiny and rebuilt every run. Facts are replaced per `year_month` partition for months touched by new data. If the
product table changes, all fact months are rebuilt because unit cost feeds cost and profit. Marts are small and fully recomputed
from the star schema, which makes them trivially idempotent.

## 8. Marts as SQL files
Each `sql/marts/mart_*.sql` is one SELECT; the file name is the table name. Analysts can change KPI logic without touching
Python, and the same SQL runs on Databricks SQL. Only two placeholders exist (`${abc_a}`, `${abc_b}`), filled from config.

## 9. Parquet locally, Delta on Azure
Same four storage operations on both. The Parquet upsert is not atomic (read partitions, merge, overwrite); Delta `MERGE` and
`replaceWhere` are transactional. The Delta path is implemented and has a test, but could not be executed in the environment where
this project was built (Delta jars come from Maven), so it is verified by a non-blocking CI job and `make delta`.

## 10. Failure modes
| Failure | Behaviour |
|---|---|
| Crash after Bronze, before Silver | State says Silver is pending; next run resumes it |
| Crash during a Parquet partition overwrite | Partition may be missing until the next run recreates it (Delta avoids this) |
| Same file dropped twice or renamed | Skipped by SHA-256 checksum |
| Source adds an unexpected column | Ignored (only contract columns are read) |
| Source drops a required column | Run fails fast with the missing column names |
| Two runs at the same time | Not supported (JSON state file has no locking) |

## 11. Performance notes (design, not measurements)
No benchmark numbers are claimed; the sample has about 3,300 rows and local runtime is dominated by Spark start-up. Silver and Gold
facts are partitioned by month so upserts touch only affected partitions. Small dimensions are broadcast in joins. On Delta, run
`OPTIMIZE ... ZORDER BY` on large tables and consider liquid clustering.

## 12. Likely interview questions
**Why a star schema on a lakehouse?** Business users and BI tools reason in facts and dimensions; conformed dimensions make
every KPI consistent across reports.
**How do you know the numbers are right?** The generator computes expected counts, revenue and profit independently; the
integration test compares them to the Gold tables, and the inventory reconciliation cross-checks sales against stock.
**What if a product's cost changes?** All fact months are rebuilt in this version (SCD1); the proper answer is SCD2.
**What would you change at 100x scale?** Auto Loader for ingestion, Delta control table for state, partition pruning on MERGE,
metrics in a single aggregation pass, incremental marts.
