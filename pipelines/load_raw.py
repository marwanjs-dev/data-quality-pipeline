import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def main() -> None:
    load_dotenv()  # reads .env in repo root if present

    # Connection info from .env
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5433")
    db   = os.getenv("PGDATABASE", "warehouse")
    user = os.getenv("PGUSER", "warehouse")
    pwd  = os.getenv("PGPASSWORD", "warehouse")

    engine = create_engine(f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}")

    landing = Path("data/landing")
    zones_path = landing / "taxi_zone_lookup.csv"

    # Auto-pick the first yellow parquet file in data/landing/
    parquet_files = sorted(landing.glob("yellow_tripdata_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No file matching data/landing/yellow_tripdata_*.parquet found.")
    trips_path = parquet_files[0]

    if not zones_path.exists():
        raise FileNotFoundError("Missing data/landing/taxi_zone_lookup.csv")

    print(f"Using trips file: {trips_path.name}")
    print(f"Using zones file: {zones_path.name}")

    # Create schemas
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

    # Load zones
    zones = pd.read_csv(zones_path)
    zones.columns = [c.strip().lower() for c in zones.columns]
    zones.to_sql("zones", engine, schema="raw", if_exists="replace", index=False)

    # Load trips
    trips = pd.read_parquet(trips_path)
    trips.columns = [c.strip().lower() for c in trips.columns]
    trips.to_sql(
        "yellow_trips",
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=50_000,
        method="multi",
    )

    print("Loaded raw.zones and raw.yellow_trips successfully.")

if __name__ == "__main__":
    main()
