import os
from datetime import datetime, timedelta

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request, AuthorizedSession

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from google.api_core.exceptions import NotFound

def main():
    # --- Authentication / API Setup ---
    SCOPES = ''
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '',
                SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    authed_session = AuthorizedSession(creds)

    # --- Parameters / Date Range ---
    base_url = ""
    location_ids = [
    ]
    # Example hard-coded range; adjust as needed
    params = [
        ('dailyMetrics', 'BUSINESS_IMPRESSIONS_DESKTOP_MAPS'),
        ('dailyMetrics', 'BUSINESS_IMPRESSIONS_DESKTOP_SEARCH'),
        ('dailyMetrics', 'BUSINESS_IMPRESSIONS_MOBILE_MAPS'),
        ('dailyMetrics', 'BUSINESS_IMPRESSIONS_MOBILE_SEARCH'),
        ('dailyMetrics', 'BUSINESS_CONVERSATIONS'),
        ('dailyMetrics', 'BUSINESS_DIRECTION_REQUESTS'),
        ('dailyMetrics', 'CALL_CLICKS'),
        ('dailyMetrics', 'WEBSITE_CLICKS'),
        ('dailyMetrics', 'BUSINESS_BOOKINGS'),
        ('dailyMetrics', 'BUSINESS_FOOD_ORDERS'),
        ('dailyMetrics', 'BUSINESS_FOOD_MENU_CLICKS'),
        ('dailyRange.start_date.year', '2024'),
        ('dailyRange.start_date.month', '01'),
        ('dailyRange.start_date.day', '01'),
        ('dailyRange.end_date.year', '2025'),
        ('dailyRange.end_date.month', '05'),
        ('dailyRange.end_date.day', '01'),
    ]

    # --- Fetch & Transform Metrics ---
    all_rows = []
    metric_names = []

    for loc_id in location_ids:
        print(f"Processing {loc_id}")
        endpoint = (
            f"{base_url}/locations/{loc_id}:"
            "fetchMultiDailyMetricsTimeSeries"
        )
        resp = authed_session.get(endpoint, params=params)
        if resp.status_code != 200:
            print(f"Error for {loc_id}: {resp.status_code} {resp.text}")
            continue

        data = resp.json().get("multiDailyMetricTimeSeries", [])
        if not data:
            print(f"No data for {loc_id}")
            continue

        # Capture metric names once
        if not metric_names:
            for m in data[0].get("dailyMetricTimeSeries", []):
                metric_names.append(m.get("dailyMetric"))

        # Build rows per date × location
        for m in data[0].get("dailyMetricTimeSeries", []):
            name = m.get("dailyMetric")
            for entry in m.get("timeSeries", {}).get("datedValues", []):
                d = entry["date"]
                date_str = f"{d['year']}-{d['month']:02d}-{d['day']:02d}"
                # find or create the row
                row = next(
                    (r for r in all_rows
                     if r["date"] == date_str and r["profile_id"] == loc_id),
                    None
                )
                if not row:
                    row = {"date": date_str, "profile_id": loc_id}
                    all_rows.append(row)
                # set the metric value (int or None)
                val = entry.get("value")
                try:
                    row[name] = int(val) if val is not None else None
                except (TypeError, ValueError):
                    row[name] = val

    # --- BigQuery Setup ---
    bq = bigquery.Client()
    proj = bq.project
    ds_id = ''
    tbl_id = ''
    ds_ref = bigquery.DatasetReference(proj, ds_id)
    tbl_ref = ds_ref.table(tbl_id)

    # Ensure dataset exists
    try:
        bq.get_dataset(ds_ref)
    except NotFound:
        bq.create_dataset(bigquery.Dataset(ds_ref))
        print(f"Created dataset {ds_id}")

    # Define schema
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("profile_id", "STRING")
    ] + [
        bigquery.SchemaField(m, "INTEGER", mode="NULLABLE")
        for m in sorted(set(metric_names))
    ]

    # Ensure table exists (optional; load job can create it too)
    try:
        bq.get_table(tbl_ref)
    except NotFound:
        bq.create_table(bigquery.Table(tbl_ref, schema=schema))
        print(f"Created table {tbl_id}")

    # --- Overwrite via Load Job ---
    job_config = LoadJobConfig(
        schema=schema,
        write_disposition=WriteDisposition.WRITE_TRUNCATE
    )
    load_job = bq.load_table_from_json(
        all_rows,
        tbl_ref,
        job_config=job_config
    )
    load_job.result()  # wait for completion
    print(f"Table {tbl_id} overwritten with {len(all_rows)} rows.")

if __name__ == '__main__':
    main()
