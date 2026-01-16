import great_expectations as gx
import sys


SUITE_NAME = "fct_trips_suite"
CHECKPOINT_NAME = "fct_trips_checkpoint"

def main() -> None:
    context = gx.get_context()

    # Use the datasource + asset you registered earlier
    datasource = context.get_datasource("pg_docker")
    asset = datasource.get_asset("fct_trips")

    # Whole-table batch request
    batch_request = asset.build_batch_request()

    # Create/update a checkpoint that runs your suite on that batch
    checkpoint = context.add_or_update_checkpoint(
        name=CHECKPOINT_NAME,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": SUITE_NAME,
            }
        ],
    )

    # Run validation (this is the "gate")
    results = checkpoint.run()
    if not results["success"]:
        print("Data quality gate FAILED. Exiting with code 1.")
        sys.exit(1)

    # Build HTML Data Docs (the report)
    context.build_data_docs()
    urls = context.get_docs_sites_urls()
    # if urls:
    #     print("Open the report here (local Data Docs):")
    #     for u in urls:
    #         print(u["site_name"], "->", u["site_url"])

if __name__ == "__main__":
    main()
