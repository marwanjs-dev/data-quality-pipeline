# data-quality-pipeline

Builds an end-to-end SQL-first pipeline that ingests NYC Taxi trip data into Postgres, transforms it into analytics and feature tables with dbt, enforces automated data quality gates with Great Expectations, detects data drift with Evidently, and publishes HTML reports—failing CI when checks regress.