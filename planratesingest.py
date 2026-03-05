import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import psycopg
import yaml


def load_config() -> dict:
    """Loads config.yaml from the same folder as this script."""
    config_path = Path(__file__).resolve().parent / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "database" not in cfg:
        raise ValueError("config.yaml must contain a top-level 'database' object")

    return cfg


def connect(cfg: dict) -> psycopg.Connection:
    """Connects using database.name or database.dbname."""
    db = cfg["database"]
    dbname = db.get("dbname") or db.get("name")
    if not dbname:
        raise ValueError("config.yaml missing database.dbname or database.name")

    missing = [k for k in ("host", "port", "user", "password") if k not in db]
    if missing:
        raise ValueError(f"config.yaml missing database fields: {missing}")

    return psycopg.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=dbname,
        user=db["user"],
        password=db["password"],
    )


def sha256_file(path: Path) -> str:
    """Computes file hash for idempotency."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_rates(doc: Any) -> list[dict]:
    """
    Supports:
      - { rates: [ ... ] }
      - [ ... ]
    """
    if isinstance(doc, dict) and isinstance(doc.get("rates"), list):
        rates = doc["rates"]
    elif isinstance(doc, list):
        rates = doc
    else:
        raise ValueError("YAML must be a list OR a dict with key 'rates' as a list")

    # Ensure each rate is a dict
    out: list[dict] = []
    for i, r in enumerate(rates, start=1):
        if not isinstance(r, dict):
            raise ValueError(f"Rate at position {i} is not a mapping/object: {r}")
        out.append(r)
    return out


def main() -> None:
    cfg = load_config()

    yaml_path = Path(__file__).resolve().parent / "planrateupdates.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"planrateupdates.yaml not found at: {yaml_path}")

    doc = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    rates = extract_rates(doc)

    pipeline_name = "plan_rates_yaml"
    file_hash = sha256_file(yaml_path)

    with connect(cfg) as conn:
        with conn:  # transaction boundary
            with conn.cursor() as cur:
                # 1) Idempotent skip if this exact file already succeeded
                cur.execute(
                    """
                    SELECT batch_id, status
                    FROM enerweb.ingestion_batch
                    WHERE pipeline_name = %s AND file_sha256 = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (pipeline_name, file_hash),
                )
                existing = cur.fetchone()
                if existing and existing[1] == "SUCCEEDED":
                    print(f"SKIP: file already ingested. batch_id={existing[0]}")
                    return

                # 2) Create batch
                batch_id = uuid.uuid4()
                cur.execute(
                    """
                    INSERT INTO enerweb.ingestion_batch
                      (batch_id, pipeline_name, source_system, source_ref, file_sha256, status)
                    VALUES
                      (%s, %s, %s, %s, %s, %s)
                    """,
                    (str(batch_id), pipeline_name, "local", str(yaml_path), file_hash, "RUNNING"),
                )

                # 3) Stage rates (no silent fixes)
                stage_rows = []
                for row_num, r in enumerate(rates, start=1):
                    stage_rows.append(
                        (
                            str(batch_id),
                            row_num,
                            r.get("plan_code"),
                            r.get("effective_date"),
                            r.get("rate_cents_per_kwh"),
                            json.dumps(r),
                        )
                    )

                cur.executemany(
                    """
                    INSERT INTO enerweb.stg_plan_rate
                      (batch_id, row_num, plan_code, effective_date, rate_cents_per_kwh, raw_yaml)
                    VALUES
                      (%s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    stage_rows,
                )

                # 4) Finalize: validate -> reject -> merge -> metrics
                cur.execute("SELECT enerweb.finalize_plan_rate_batch(%s)", (str(batch_id),))

                # 5) Mark batch as succeeded
                cur.execute(
                    """
                    UPDATE enerweb.ingestion_batch
                    SET status='SUCCEEDED', finished_at=now(), error_message=NULL
                    WHERE batch_id=%s
                    """,
                    (str(batch_id),),
                )

                # 6) Print metrics for this batch (avoid LIKE 'rate_%' due to psycopg % parsing)
                cur.execute(
                    """
                    SELECT metric_name, metric_value
                    FROM enerweb.ingestion_batch_metric
                    WHERE batch_id=%s
                      AND LEFT(metric_name, 5) = 'rate_'
                    ORDER BY metric_name
                    """,
                    (str(batch_id),),
                )

                print(f"\nBatch: {batch_id}")
                for name, value in cur.fetchall():
                    print(f"  {name}: {value}")

                # 7) Print rejects (if any)
                cur.execute(
                    """
                    SELECT row_num, reason, raw_yaml
                    FROM enerweb.reject_plan_rate
                    WHERE batch_id=%s
                    ORDER BY row_num
                    """,
                    (str(batch_id),),
                )
                rejects = cur.fetchall()

                print("\nRejects:")
                if not rejects:
                    print("  (none)")
                else:
                    for row_num, reason, raw_yaml in rejects:
                        print(f"  row_num={row_num} reason={reason} raw={raw_yaml}")

    print("\nDone.")


if __name__ == "__main__":
    main()