#!/usr/bin/env python3
"""
Run the VCA raw SQL from an external .sql file and save results to CSV.

Reads DB credentials from a `.env` file located in the SAME FOLDER as this script.

Supported keys in .env (either use DB_URL OR the parts):
  DB_URL=postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DBNAME
  DB_HOST=localhost
  DB_PORT=5432
  DB_USER=postgres
  DB_PASSWORD=postgres
  DB_NAME=postgres

Usage:
  python run_vca_extract.py --sql vca_raw_extract.sql --out vca_raw_extract.csv
"""

import os
import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text


def load_env_from_file(env_path: Path) -> None:
    """Minimal .env loader (no external deps). Adds keys to os.environ if not set."""
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        # Only set if not already set in environment
        os.environ.setdefault(k, v)


def build_db_url() -> str:
    """Build SQLAlchemy URL from environment (.env already loaded)."""
    # Highest priority: full URL
    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url

    # Otherwise assemble from parts
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    pwd  = os.getenv("DB_PASSWORD", "postgres")
    db   = os.getenv("DB_NAME", "postgres")

    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", required=True, help="Path to .sql file containing the query")
    parser.add_argument("--out", required=True, help="Path to write CSV output")
    args = parser.parse_args()

    # Load .env that sits next to this script
    script_dir = Path(__file__).resolve().parent
    load_env_from_file(script_dir / ".env")

    # Build DB URL from env (populated by .env loader)
    db_url = build_db_url()

    # Read SQL
    sql_path = Path(args.sql)
    sql_text = sql_path.read_text(encoding="utf-8")

    # Execute and export
    engine = create_engine(db_url, future=True)
    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql_text), conn)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {len(df):,} rows to {args.out}")


if __name__ == "__main__":
    main()
