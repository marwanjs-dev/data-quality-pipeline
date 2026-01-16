import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# Custom method for faster insertions using COPY
def psql_insert_copy(table, conn, keys, data_iter):
    import csv
    from io import StringIO

    # Access the DBAPI connection (psycopg)
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        
        # Use psycopg's copy context manager
        if hasattr(cur, 'copy'):
            with cur.copy(sql) as copy:
                copy.write(s_buf.read())
        else:
            # Fallback (e.g. if using psycopg2 in future)
            cur.copy_expert(sql=sql, file=s_buf)

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

    # print(f"Using trips file: {trips_path.name}")
    # print(f"Using zones file: {zones_path.name}")

    # Create schemas
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

    insp = inspect(engine)


    # Load zones
    zones = pd.read_csv(zones_path)
    zones.columns = [c.strip().lower() for c in zones.columns]
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

    if insp.has_table("zones", schema="raw"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.zones;"))
        zones.to_sql("zones", engine, schema="raw", if_exists="append", index=False)
    else:
        zones.to_sql("zones", engine, schema="raw", if_exists="replace", index=False)


    # Load trips
    trips = pd.read_parquet(trips_path)
    


    trips.columns = [c.strip().lower() for c in trips.columns]

    print("Loading yellow_trips using COPY method...")
    
    if insp.has_table("yellow_trips", schema="raw"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.yellow_trips;"))
        trips.to_sql(
            "yellow_trips",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method=psql_insert_copy,  # Much faster
            chunksize=100000,         # Larger chunks are fine for COPY
        )
    else:
        trips.to_sql(
            "yellow_trips",
            engine,
            schema="raw",
            if_exists="replace",
            index=False,
            method=psql_insert_copy,
            chunksize=100000,
        )

    print("Loaded raw.zones and raw.yellow_trips successfully.")

if __name__ == "__main__":
    main()
