import argparse
import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def ensure_schemas(engine: Engine):
    """Create standard schemas if missing."""
    for schema in ("house_prices_raw", "house_prices_staging", "house_prices_marts"):
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))


def load_csv_to_postgres(
    csv_path: str,
    engine: Engine,
    schema: str,
    table: str,
    if_exists: str = "fail",
    chunksize: Optional[int] = 50_000,
):
    """
    Load CSV into Postgres using pandas.to_sql with optional chunking.

    Args:
        csv_path: path to CSV
        engine: SQLAlchemy engine
        schema: target schema ("raw" by default)
        table: target table name
        if_exists: "fail" | "replace" | "append"
        chunksize: rows per chunk; None loads in one go
    """
    # Read once to infer columns/types
    df_iter = pd.read_csv(
        csv_path,
        low_memory=False,  # avoid mixed dtypes
        iterator=True,
        chunksize=chunksize or 100_000,
    )

    first = True
    total_rows = 0
    for chunk in df_iter:
        # Preserve raw values
        chunk.to_sql(
            table,
            engine,
            schema=schema,
            if_exists=("replace" if first and if_exists != "append" else "append"),
            index=False,
            method="multi",
        )
        total_rows += len(chunk)
        first = False

    return total_rows


def main():
    parser = argparse.ArgumentParser(description="Load raw CSV into Postgres (raw schema).")
    parser.add_argument("--csv", required=True, help="Path to Kaggle train.csv")
    parser.add_argument(
        "--url",
        required=False,
        default=os.getenv("PG_URL", "postgresql://admin:admin@localhost:5432/house_prices"),
        help="SQLAlchemy Postgres URL",
    )
    parser.add_argument("--schema", default="house_prices_raw", help="Target schema, default 'raw'")
    parser.add_argument("--table", default="raw_house_prices", help="Target table name")
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="replace",
        help="Behavior if table exists",
    )
    parser.add_argument("--chunksize", type=int, default=50_000, help="Chunk size for loading")

    args = parser.parse_args()

    engine = create_engine(args.url)
    ensure_schemas(engine)

    rows = load_csv_to_postgres(
        csv_path=args.csv,
        engine=engine,
        schema=args.schema,
        table=args.table,
        if_exists=args.if_exists,
        chunksize=args.chunksize,
    )

    print(f"Loaded {rows} rows into {args.schema}.{args.table}")


if __name__ == "__main__":
    main()