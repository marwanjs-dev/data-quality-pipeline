import great_expectations as gx

SUITE_NAME = "fct_trips_suite"

def main() -> None:
    context = gx.get_context()

    # Get datasource + asset we registered
    datasource = context.get_datasource("pg_docker")
    asset = datasource.get_asset("fct_trips")

    # Build a batch request for the whole table
    batch_request = asset.build_batch_request()

    # Create (or update) the suite
    suite = context.add_or_update_expectation_suite(expectation_suite_name=SUITE_NAME)

    # Create a validator so we can add expectations against the data
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME,
    )

    # Core expectations (start small but meaningful)
    validator.expect_column_values_to_not_be_null("pickup_ts")
    validator.expect_column_values_to_not_be_null("dropoff_ts")
    validator.expect_column_values_to_be_between("trip_distance", min_value=0, strictly=True)
    validator.expect_column_values_to_be_between("total_amount", min_value=0)

    # Save suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"Created/updated suite: {SUITE_NAME}")

if __name__ == "__main__":
    main()
