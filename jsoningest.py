import hashlib
import io
import csv
import json
import uuid
from pathlib import Path

import psycopg
import yaml


def load_config() -> dict:
      # Load config.yaml from the same folder as this script.
    
    config_path = Path(__file__).resolve().parent / "config.yaml"
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def connect(cfg: dict) -> psycopg.Connection:
    
    # Build a Postgres connection using config.yaml.
    # Supports either "dbname" or "name" for the database key.
    db = cfg["database"]
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


def sha256_file(path: Path) -> str:
    # Compute a file hash used for idempotency:
    # sha256(file) + pipeline_name -> unique load identity.
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_stage(cur: psycopg.Cursor, batch_id: str, readings: list[dict]) -> None:
    """
    psycopg3-correct COPY FROM STDIN.
    We stream CSV rows into COPY using the context manager.
    """
    # Use an in-memory buffer so we can flush every N rows and avoid huge memory spikes.

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

    row_num = 0
    for r in readings:
        row_num += 1
        w.writerow(
            [
                batch_id,
                row_num,
                r.get("customer_id") or "",
                r.get("meter_id") or "",
                r.get("plan_code") or "",
                r.get("reading_date") or "",
                "" if r.get("kwh") is None else r.get("kwh"),
                r.get("source_ts") or "",
                json.dumps(r),
            ]
        )

        # Flush buffer in chunks to avoid huge memory spikes
        if row_num % 10000 == 0:
            buf.seek(0)
            data = buf.read()
            buf.seek(0)
            buf.truncate(0)
            yield data

    # Final flush
    buf.seek(0)
    remaining = buf.read()
    if remaining:
        yield remaining


def main() -> None:
    # Load configuration (DB credentials + assumptions live outside the code).
    cfg = load_config()

    # Input JSON file (must contain a top-level "readings" array).
    json_path = Path(__file__).resolve().parent / "meterreadings.json"
    if not json_path.exists():
        raise FileNotFoundError(f"Input file not found: {json_path}")

    print("Step 1: Read input JSON file")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    readings = payload.get("readings", [])
    if not isinstance(readings, list):
        raise ValueError("Expected JSON to contain key 'readings' as a list")
    print(f"  Records in file: {len(readings)}")
    
    # Pipeline identity (used with file hash to enforce idempotency).

    pipeline_name = "meterreadings_json"
    
    # File fingerprint for idempotency: same pipeline + same file hash should not ingest twice.
    file_hash = sha256_file(json_path)
    print("Step 2: Compute file hash for idempotency")
    print(f"  sha256: {file_hash}")

    print("Step 3: Connect to database")
    with connect(cfg) as conn:
        with conn:
            with conn.cursor() as cur:
                
                # Prevent "hanging forever" if another session holds locks.
                cur.execute("SET lock_timeout = '5s';")

                cur.execute("SELECT current_database(), current_user;")
                dbname, dbuser = cur.fetchone()
                print(f"  Connected to database: {dbname} as user: {dbuser}")

                print("Step 4: Check if this file was already ingested")
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
                
                # Idempotent skip: if a SUCCEEDED batch exists for this file, do nothing.
                if existing and existing[1] == "SUCCEEDED":
                    print(f"  Already ingested. Skipping. batch_id={existing[0]}")
                    return

                print("Step 5: Create ingestion batch record")
                
                # Each ingestion run gets a unique batch_id for auditability.
                
                batch_id = uuid.uuid4()
                cur.execute(
                    """
                    INSERT INTO enerweb.ingestion_batch
                      (batch_id, pipeline_name, source_system, source_ref, file_sha256, status)
                    VALUES
                      (%s, %s, %s, %s, %s, %s)
                    """,
                    (str(batch_id), pipeline_name, "local", str(json_path), file_hash, "RUNNING"),
                )
                print(f"  Created batch_id: {batch_id}")

                print("Step 6: Stage records using COPY")
                
                # COPY is the high-performance bulk loader.
                # NULL '' tells Postgres: empty fields should be treated as SQL NULL.
                
                copy_sql = """
                    COPY enerweb.stg_meter_reading
                      (batch_id, row_num, customer_id, meter_id, plan_code, reading_date, kwh, source_ts, raw_json)
                    FROM STDIN WITH (FORMAT csv, NULL '')
                """

                staged_stream = copy_stage(cur, str(batch_id), readings)
                with cur.copy(copy_sql) as cp:
                    for chunk in staged_stream:
                        cp.write(chunk)
                        
                # Confirm staging row count for this batch
                cur.execute(
                    "SELECT COUNT(*) FROM enerweb.stg_meter_reading WHERE batch_id=%s",
                    (str(batch_id),),
                )
                staged = cur.fetchone()[0]
                print(f"  Staged rows: {staged}")

                # If staging is empty, something went wrong with COPY or input mapping.

                if staged == 0:
                    raise RuntimeError(
                        "COPY staged 0 rows. This indicates COPY did not receive data. "
                        "Check that meterreadings.json has 'readings' and that COPY executed."
                    )

                print("Step 7: Finalize batch (validate, reject, merge, metrics)")
                # Database-side function performs:
                # - validation
                # - reject routing (reject_meter_reading)
                # - merge into core tables (customer, meter, meter_reading)
                # - metrics into ingestion_batch_metric
                cur.execute("SELECT enerweb.finalize_meter_reading_batch(%s)", (str(batch_id),))

                print("Step 8: Mark batch as SUCCEEDED")
                # Mark the batch complete only after finalize succeeds.

                cur.execute(
                    """
                    UPDATE enerweb.ingestion_batch
                    SET status='SUCCEEDED', finished_at=now(), error_message=NULL
                    WHERE batch_id=%s
                    """,
                    (str(batch_id),),
                )

                print("Step 9: Show batch metrics")
                cur.execute(
                    """
                    SELECT metric_name, metric_value
                    FROM enerweb.ingestion_batch_metric
                    WHERE batch_id=%s
                    ORDER BY metric_name
                    """,
                    (str(batch_id),),
                )
                for name, value in cur.fetchall():
                    print(f"  {name}: {value}")

                print("Step 10: Show reject count only")
                cur.execute(
                    "SELECT COUNT(*) FROM enerweb.reject_meter_reading WHERE batch_id=%s",
                    (str(batch_id),),
                )
                reject_count = cur.fetchone()[0]
                print(f"  rejected_rows: {reject_count}")
                
                # High-level counts to confirm merge into core tables occurred.

                print("Step 11: Core table counts (sanity)")
                cur.execute("SELECT COUNT(*) FROM enerweb.customer;")
                print(f"  customers: {cur.fetchone()[0]}")
                cur.execute("SELECT COUNT(*) FROM enerweb.meter;")
                print(f"  meters: {cur.fetchone()[0]}")
                cur.execute("SELECT COUNT(*) FROM enerweb.meter_reading;")
                print(f"  meter_readings: {cur.fetchone()[0]}")

    print("Done.")


if __name__ == "__main__":
    main()