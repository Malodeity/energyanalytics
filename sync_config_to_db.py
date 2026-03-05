import psycopg
import yaml
from pathlib import Path


def load_config() -> dict:
    # Read config.yaml from the same folder as this script
    config_path = Path(__file__).resolve().parent / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ValueError("config.yaml must be a YAML mapping (top-level dictionary)")

    return cfg


def connect(cfg: dict) -> psycopg.Connection:
    db = cfg.get("database", {})
    dbname = db.get("dbname") or db.get("name")
    if not dbname:
        raise ValueError("config.yaml missing database.dbname or database.name")

    return psycopg.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=dbname,
        user=db["user"],
        password=db["password"],
    )


def main() -> None:
    cfg = load_config()

    assumptions = cfg.get("assumptions", {})
    if not isinstance(assumptions, dict):
        raise ValueError("config.yaml assumptions must be a dictionary")

    currency = assumptions.get("currency", "ZAR")
    fallback_rate = assumptions.get("fallback_rate_cents_per_kwh")
    anomaly_threshold = assumptions.get("anomaly_threshold", 3.0)

    if fallback_rate is None:
        raise ValueError("config.yaml missing assumptions.fallback_rate_cents_per_kwh")

    print("Syncing config.yaml assumptions into enerweb.config")

    with connect(cfg) as conn:
        with conn:
            with conn.cursor() as cur:
                # Ensure schema exists (safe)
                cur.execute("CREATE SCHEMA IF NOT EXISTS enerweb;")

                # Ensure config table exists (safe)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS enerweb.config (
                      config_id SMALLINT PRIMARY KEY DEFAULT 1,
                      currency  TEXT NOT NULL DEFAULT 'ZAR',
                      fallback_rate_cents_per_kwh NUMERIC(12,3) NOT NULL,
                      anomaly_threshold NUMERIC(6,3) NOT NULL DEFAULT 3.0,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      CONSTRAINT ck_single_row CHECK (config_id = 1)
                    );
                    """
                )

                # Upsert the single config row
                cur.execute(
                    """
                    INSERT INTO enerweb.config (config_id, currency, fallback_rate_cents_per_kwh, anomaly_threshold)
                    VALUES (1, %s, %s, %s)
                    ON CONFLICT (config_id)
                    DO UPDATE SET
                      currency = EXCLUDED.currency,
                      fallback_rate_cents_per_kwh = EXCLUDED.fallback_rate_cents_per_kwh,
                      anomaly_threshold = EXCLUDED.anomaly_threshold,
                      updated_at = now();
                    """,
                    (currency, fallback_rate, anomaly_threshold),
                )

                # Print what is now in the DB
                cur.execute(
                    """
                    SELECT config_id, currency, fallback_rate_cents_per_kwh, anomaly_threshold, updated_at
                    FROM enerweb.config
                    WHERE config_id = 1;
                    """
                )
                row = cur.fetchone()

    print("Config synced successfully.")
    print("Current DB config:", row)


if __name__ == "__main__":
    main()