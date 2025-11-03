import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from datetime import datetime, timedelta

# --- Configuration ---
PROPERTY_IDS = [
    
]

# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

# BigQuery settings
BIGQUERY_PROJECT_ID = ""
BIGQUERY_DATASET_ID = ""
BIGQUERY_TABLE_ID = ""

# Table schema must match the fields below
SCHEMA = [
    bigquery.SchemaField("property_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sessions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("engaged_sessions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("event_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("key_events", "INTEGER", mode="NULLABLE"),
]


def run_ga4_report_and_load_to_bigquery(property_ids):
    analytics_client = BetaAnalyticsDataClient()
    bq_client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    dataset_ref = bq_client.dataset(BIGQUERY_DATASET_ID)
    table_ref = dataset_ref.table(BIGQUERY_TABLE_ID)

    # Ensure table exists
    try:
        bq_client.get_table(table_ref)
        print(f"Table exists: {BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}")
    except Exception:
        print(f"Table not found. Creating {BIGQUERY_TABLE_ID}...")
        table = bigquery.Table(table_ref, schema=SCHEMA)
        bq_client.create_table(table)
        print("Table created.")

    all_rows = []
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.date().isoformat()

    for prop in property_ids:
        print(f"Fetching GA4 data for property {prop} on {date_str}...")
        request = RunReportRequest(
            property=f"properties/{prop}",
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="engagedSessions"),
                Metric(name="eventCount"),
                Metric(name="keyEvents"),
            ],
            date_ranges=[DateRange(start_date='2025-05-04', end_date='2025-05-04')],
        )
        try:
            response = analytics_client.run_report(request)
            for row in response.rows:
                all_rows.append({
                    "property_id": prop,
                    "date": row.dimension_values[0].value,
                    "sessions": int(row.metric_values[0].value or 0),
                    "engaged_sessions": int(row.metric_values[1].value or 0),
                    "event_count": int(row.metric_values[2].value or 0),
                    "key_events": int(row.metric_values[3].value or 0),
                })
            print(f"Collected {len(response.rows)} rows for property {prop}.")
        except Exception as e:
            print(f"Error on property {prop}: {e}")

    if not all_rows:
        print("No data to load.")
        return

    # Configure batch load job
    job_config = LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_APPEND" 
    )

    print(f"Starting batch load of {len(all_rows)} rows to BigQuery...")
    load_job = bq_client.load_table_from_json(
        all_rows,
        table_ref,
        job_config=job_config
    )
    load_job.result()  # Wait for completion
    print(f"Loaded {load_job.output_rows} rows into {BIGQUERY_TABLE_ID}.")


if __name__ == "__main__":
    run_ga4_report_and_load_to_bigquery(PROPERTY_IDS)
