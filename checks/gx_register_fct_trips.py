import os
from dotenv import load_dotenv
import great_expectations as gx

def main() -> None:
    # Load your .env (PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD)
    load_dotenv()

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5433")
    db   = os.getenv("PGDATABASE", "warehouse")
    user = os.getenv("PGUSER", "warehouse")
    pwd  = os.getenv("PGPASSWORD", "warehouse")

    conn_string = f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"

    # Load the Great Expectations Data Context from your repo
    context = gx.get_context()

    # 1) Create/update a Postgres datasource
    datasource = context.sources.add_or_update_postgres(
        name="pg_docker",
        connection_string=conn_string,
    )

    # 2) Register the mart.fct_trips table as a Data Asset
    asset = datasource.add_table_asset(
        name="fct_trips",
        table_name="fct_trips",
        schema_name="mart",
    )

    # 3) Define the default batch as "whole table"
    # asset.add_batch_definition_whole_table("whole_table")
    # asset.(name="whole_table")

    # Persist changes (not needed in GE v3+)

    print("GX datasource + asset registered: pg_docker -> mart.fct_trips (whole_table)")

if __name__ == "__main__":
    main()
