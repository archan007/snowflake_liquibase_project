# Snowflake Platform

Config-driven Snowflake DDL platform. You describe what you want in `bundle.yaml` files and CSV schemas; the engine diffs that against live Snowflake state and emits Liquibase-compatible changesets; Liquibase applies them via CI.

> DBT handles transformations (DML only, no DDL). SODA handles data quality. Airflow orchestrates. This repo owns structure: databases, schemas, tables, views, streams, tasks, stored procs, dynamic tables, stages, file formats.

---

## Repository layout

```
.
├── bundles/                 # What you want to exist in Snowflake
│   ├── bronze/              # → BRONZE schema
│   │   ├── bundle.yaml
│   │   └── schemas/*.csv
│   ├── silver/              # → SILVER schema
│   │   ├── bundle.yaml
│   │   └── schemas/*.csv
│   └── gold/                # → GOLD schema
│       ├── bundle.yaml
│       └── products/        # Logical grouping of objects by data product
│           └── product_a/
│               ├── schemas/*.csv
│               └── sql/*.sql
├── platform/                # Account-wide config + per-env overrides
│   ├── platform.yaml
│   └── overrides/{dev,uat,prod}.yaml
├── engine/                  # Python templating engine (do not hand-edit output)
│   ├── config_loader.py
│   ├── state_reader.py
│   ├── generators/
│   └── generate_ddl.py
├── liquibase/
│   ├── changelog-master.xml # Stable root changelog (committed)
│   └── liquibase.properties
├── output/                  # Generated DDL — gitignored, rebuilt every run
├── test_drop_safety.py
├── requirements.txt
└── .github/workflows/       # pr-checks + deploy-{dev,uat,prod}
```

### Database / schema model

One Snowflake account, one database per environment, three schemas per database:

| Environment | Database     | Schemas                  |
|-------------|--------------|--------------------------|
| DEV         | `DEV_FS_DB`  | `BRONZE`, `SILVER`, `GOLD` |
| UAT         | `UAT_FS_DB`  | `BRONZE`, `SILVER`, `GOLD` |
| PROD        | `PROD_FS_DB` | `BRONZE`, `SILVER`, `GOLD` |

The database name is exposed to bundles and SQL files as `${DATABASE}` and resolved per env from `platform/overrides/<env>.yaml`. Bundles never hardcode the database name. Inside the `GOLD` schema, objects belonging to a particular data product are organised under `products/<product_id>/` for convenience — naming is **not enforced**, so users may name their gold objects however they like (e.g. `PRODUCT_A__FCT_SALES`, `FCT_SALES_PA`, or just `FCT_SALES`). Avoid collisions across products.

---

## Local quickstart (offline)

You don't need Snowflake credentials to run the engine locally. The `--offline` flag skips state reading and assumes a fresh environment.

```bash
pip install -r requirements.txt

# Generate DDL for DEV as if Snowflake were empty
python -m engine.generate_ddl --env DEV --offline --output-root output/ddl

# Run the drop-safety regression tests
python test_drop_safety.py
```

Expected: 12 changesets generated, 3/3 drop-safety tests passing.

Inspect the output:

```bash
ls output/ddl/
cat output/ddl/master.xml
```

---

## Branch strategy and promotion

Three long-lived branches, one-directional promotion:

```
feature/* ──PR──▶ dev ──cherry-pick──▶ uat ──cherry-pick──▶ main (PROD)
```

- All work starts on a feature branch and is PR'd into `dev`.
- Promotion to `uat` and `main` is by **cherry-pick**, not merge.
- CI enforces a **commit-lineage guard rail**: any commit landing on `uat` must already exist on `dev`, and any commit landing on `main` must already exist on `uat`. A direct push that skips a stage will fail the deploy.
- `main` additionally requires manual approval from the team lead / data custodian via the `prod` GitHub Environment.

---

## CI / environment variables

Each deploy workflow reads key-pair credentials from GitHub Secrets. Configure these under Settings → Secrets and variables → Actions:

**Shared:**
- `SNOWFLAKE_ACCOUNT` — e.g. `xy12345.eu-west-2.aws`

**Per environment** (prefix `SNOWFLAKE_<ENV>_`, where `<ENV>` ∈ `DEV`, `UAT`, `PROD`):
- `SNOWFLAKE_<ENV>_USER` — service user, e.g. `SVC_DEV_CI`
- `SNOWFLAKE_<ENV>_ROLE` — role with DDL grants on the target database
- `SNOWFLAKE_<ENV>_WAREHOUSE`
- `SNOWFLAKE_<ENV>_DATABASE` — `DEV_FS_DB` / `UAT_FS_DB` / `PROD_FS_DB`. The engine reads this from the per-env override file (`platform/overrides/<env>.yaml`); the secret value just needs to match so that Liquibase connects to the same database.
- `SNOWFLAKE_<ENV>_PRIVATE_KEY` — PEM-encoded private key (paste the full file contents, including `-----BEGIN PRIVATE KEY-----` headers)

Key-pair auth is manually rotated every 3 months. The CI job writes the key to a tmpfile, uses it, and shreds it on exit.

**GitHub Environments:**
- `dev`, `uat`, `prod` — create one per branch. Put the per-env secrets under the matching environment.
- On the `prod` environment, add required reviewers (data custodian, team lead) and restrict deployment branches to `main`.

**CI service users:** `SVC_DEV_CI`, `SVC_UAT_CI`, `SVC_PROD_CI` — DDL-capable, separate from Airflow's DML-only runtime users. Don't collapse these.

---

## How drop-safety works

Not all object removals are equal. The engine splits them into two groups:

**Breaking drops** — `table`, `view`, `dynamic_table`. Removing one from the bundle is potentially destructive (data loss, dependent queries breaking). The engine **refuses to generate a drop** unless you explicitly confirm it in the owning `bundle.yaml`:

```yaml
# bundles/bronze/bundle.yaml
confirmed_drops:
  - ${DATABASE}.BRONZE.RAW_LEGACY_ORPHAN
```

Without that line, the engine fails the build with:

```
[engine] ERROR: breaking-change drop detected without confirmation:
  - TABLE DEV_FS_DB.BRONZE.RAW_LEGACY_ORPHAN
  To proceed, add each FQN to the 'confirmed_drops' list ...
```

> **Use `${DATABASE}` in confirmed_drops, not a hardcoded DB name.** The engine resolves `${DATABASE}` per environment, so a single entry covers DEV, UAT, and PROD as the change is cherry-picked through. The drop is still reviewed at each PR stage (because the cherry-pick lands in a separate PR per branch), but you don't have to maintain three separate FQN strings.

This forces the person removing the object to put the FQN in the PR, which makes it visible in review.

**Silent drops** — `stream`, `task`, `stored_procedure`, `stage`, `file_format`. Removing one from the bundle just generates a `DROP` changeset without ceremony. These objects are cheap to recreate and don't hold data.

`test_drop_safety.py` locks this behaviour in as regression tests. Run it before every PR.

---

## Adding a new table

1. Drop a CSV in the bundle's `schemas/` folder with the column definitions:

   ```
   bundles/bronze/schemas/raw_orders.csv
   ```

   ```csv
   column_name,data_type,nullable,default_value,rule,transform_logic,clustering_key,business_key,description
   ORDER_ID,NUMBER(18,0),false,,,,,true,Primary key
   CUSTOMER_ID,NUMBER(18,0),false,,,,,,FK to customers
   ORDER_TOTAL,"NUMBER(18,2)",false,,,,,,Total in GBP
   CREATED_AT,TIMESTAMP_NTZ,false,,,,,,Ingest timestamp
   ```

   > **Quote anything with a comma inside parentheses** like `"NUMBER(18,2)"`. The engine's CSV validator will fail the build with a clear error if you forget.

2. Reference it in `bundle.yaml`:

   ```yaml
   tables:
     - name: RAW_ORDERS
       schema: BRONZE
       description: "Raw orders landed from S3"
       schema_csv: schemas/raw_orders.csv
   ```

3. Run the engine locally to sanity-check:

   ```bash
   python -m engine.generate_ddl --env DEV --offline --output-root output/ddl
   ```

4. Open a PR against `dev`. The `pr-checks` workflow will run the engine in offline mode across all three envs and post a summary.

### Adding a column

Just edit the CSV. The engine's table generator detects column-level diffs and emits `ALTER TABLE ... ADD COLUMN` — never `CREATE OR REPLACE TABLE`, so existing data is preserved.

### Removing a column

Same: edit the CSV to drop the row. Column removal is treated as a breaking change and must be reviewed carefully — verify no downstream DBT models or consumers depend on it before merging.

### Using `${DATABASE}` in user SQL files

When you write SQL files for views, dynamic tables, stored procedures, or tasks, **never hardcode the database name**. Use `${DATABASE}` and the engine will resolve it per environment at generation time:

```sql
-- bundles/gold/products/product_a/sql/vw_sales_summary.sql
SELECT
    ORDER_DATE,
    SUM(REVENUE_USD) AS TOTAL_REVENUE
FROM ${DATABASE}.GOLD.FCT_SALES
GROUP BY ORDER_DATE
```

Resolves to `DEV_FS_DB.GOLD.FCT_SALES` in DEV, `UAT_FS_DB.GOLD.FCT_SALES` in UAT, etc. The same goes for `confirmed_drops` entries in `bundle.yaml` — use `${DATABASE}.SCHEMA.OBJECT_NAME` and one entry covers all environments.

`${ENV}` and `${ENV_LOWER}` are also available, plus anything you add to a `variables:` block in the platform overrides.

---

## Related docs

- Full blueprint: see the platform blueprint doc (link in the team wiki)
- Drop-safety rationale and worked examples: `test_drop_safety.py`
- Engine internals: docstrings in `engine/generate_ddl.py`

---

## Troubleshooting

**Engine fails with `breaking-change drop detected`** — You (or someone) removed a table/view/dynamic_table from a bundle without adding its FQN to `confirmed_drops`. Either put it back or confirm the drop explicitly.

**CSV validator complains about `NUMBER(18,2)`** — Unquoted comma inside a CSV field. Wrap the data type in double quotes: `"NUMBER(18,2)"`.

**UAT deploy fails with `Lineage guard FAILED`** — A commit landed on `uat` that isn't on `dev`. Don't push directly to `uat`; cherry-pick from `dev`.

**PROD deploy is stuck** — Waiting for a reviewer to approve in the `prod` GitHub Environment. Ping the data custodian.
