
---

# Energy Analytics

> A local pipeline for ingesting and analyzing energy consumption data and plan rates using PostgreSQL.

## Getting Started

### Prerequisites

* **Python 3.x** installed.
* **PostgreSQL** instance running locally.
* A database named `energy_analytics` created.

## Files in this repo (already provided)
- `config.yaml` (DB connection + assumptions)
- `requirements.txt` (Python dependencies)
- `meterreadings.json` (meter readings input)
- `planrateupdates.yaml` (plan rate updates input)
- `jsoningest.py` (meter readings ingestion)
- `planratesingest.py` (plan rate ingestion)
- `generatefakedata.py` (optional: regenerate test JSON)
- `SOLUTION.md` (design notes)

### Installation & Setup

1. **Initialize Database Schema** Run the schema script to set up the necessary tables and views:
```bash
psql -d energy_analytics -f sqlschema.sql

```


2. **Environment Setup** Install the required Python libraries:
```bash
pip install -r requirements.txt

```


3. **Configuration** Open `config.yaml` and update the credentials to match your local environment:
* `host`, `port`, `user`, `password`, `database`

---

### 1. Install Dependencies

It is recommended to use a virtual environment:

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# Install packages
pip install -r requirements.txt

```

---

## Data Pipeline

### 2. Sync Configuration

The analytics view relies on values defined in `enerweb.config`. Ensure your configuration is synced:

```bash
python sync_config_to_db.py

```

### 3. Ingest Plan Rates

Load the pricing tiers and effective dates:

```bash
python planratesingest.py

```

### 4. Ingest Meter Readings

Process the raw JSON consumption data:

```bash
python jsoningest.py

```

---

## Analytics & Verification

### Stakeholder View

The primary interface for results is the `enerweb.v_daily_costs` view. You can query it to see calculated costs and anomalies:

```sql
SELECT 
    customer_id, 
    meter_id, 
    reading_date, 
    kwh, 
    applied_rate_cents_per_kwh, 
    cost_cents,
    anomaly_flag
FROM enerweb.v_daily_costs
ORDER BY reading_date DESC
LIMIT 50;

```

### Quality Check

To verify the health of the ingestion, check the batch metrics:

```sql
SELECT * FROM enerweb.ingestion_batch_metric;
SELECT COUNT(*) FROM enerweb.reject_meter_reading;

```

---

## Development Tools

* **Regenerate Data:** Run `python generatefakedata.py` to create a fresh `meterreadings.json`.
* **Factory Reset:** To wipe all data while keeping the schema intact, run:
```sql
TRUNCATE TABLE 
  enerweb.meter_reading, enerweb.meter, enerweb.customer, 
  enerweb.ingestion_batch, enerweb.ingestion_batch_metric 
RESTART IDENTITY CASCADE;

```



---

