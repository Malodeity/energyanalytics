
---

# Energy Analytics

> A local pipeline for ingesting and analyzing energy consumption data and plan rates using PostgreSQL.

## Prerequisites

* **Python 3.x**
* **PostgreSQL** running locally
* Database created: `energy_analytics`

## Files in this Repo

* `sqlschema.sql` — Database schema, functions, and views.
* `config.yaml` — Connection details and business logic assumptions.
* `requirements.txt` — Python dependencies.
* `meterreadings.json` — Input meter reading data.
* `planrateupdates.yaml` — Input plan rate data.
* `sync_config_to_db.py` — Syncs YAML config to the database.
* `planratesingest.py` & `jsoningest.py` — Ingestion scripts.
* `generatefakedata.py` — (Optional) Data generator.
* `SOLUTION.md` — Design and architecture notes.

---

## ⚙️ Setup

### 1. Initialize Database Schema

Run the schema script to set up tables, views, and functions:

```bash
psql -d energy_analytics -f sqlschema.sql

```

### 2. Install Dependencies

```bash
pip install -r requirements.txt

```

### 3. Update Configuration

Edit `config.yaml` with your local PostgreSQL credentials:

* **host**, **port**, **user**, **password**, and **name** (or `dbname`).

### 4. Sync Configuration to DB

The analytics view relies on values stored in the `enerweb.config` table. Sync them by running:

```bash
python sync_config_to_db.py

```

---

##  Run the Pipeline

### 5. Ingest Plan Rates

```bash
python planratesingest.py

```

### 6. Ingest Meter Readings

```bash
python jsoningest.py

```

---

## Query Results

The primary interface for cost analysis is the `enerweb.v_daily_costs` view.

```sql
SELECT
  customer_id,
  meter_id,
  reading_date,
  kwh,
  rate_source,
  applied_rate_cents_per_kwh,
  cost_cents,
  cost_currency,
  anomaly_flag
FROM enerweb.v_daily_costs
ORDER BY reading_date DESC
LIMIT 50;

```

---

## Verification & Utilities

### Data Quality Metrics

Check ingestion health and rejection counts:

```sql
-- Batch Metrics
SELECT * FROM enerweb.ingestion_batch_metric ORDER BY recorded_at DESC;

-- Reject Counts
SELECT COUNT(*) FROM enerweb.reject_meter_reading;
SELECT COUNT(*) FROM enerweb.reject_plan_rate;

```

### Development Utilities

* **Regenerate Data:** `python generatefakedata.py`
* **Factory Reset:** To wipe all data and staging tables during development:
```sql
DROP VIEW IF EXISTS enerweb.v_daily_costs;

TRUNCATE TABLE
  enerweb.meter_reading,
  enerweb.meter,
  enerweb.customer,
  enerweb.stg_meter_reading,
  enerweb.reject_meter_reading,
  enerweb.stg_plan_rate,
  enerweb.reject_plan_rate,
  enerweb.ingestion_batch_metric,
  enerweb.ingestion_batch
RESTART IDENTITY;

```



---
