import os
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request, AuthorizedSession
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def main():
    # Define the required scope.
    SCOPES = ['https://www.googleapis.com/auth/business.manage']
    # Set your Google Cloud credentials (if not already set in your environment)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
    creds = None
    # Load credentials if available.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If no valid credentials, start the OAuth flow.
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

    # Create an authorized session.
    authed_session = AuthorizedSession(creds)

    # Base URL for the API
    base_url = "https://businessprofileperformance.googleapis.com/v1"
    
    # List of location IDs (using only the unique part; we add the "locations/" prefix in the endpoint)
    location_ids = [
        ""
    ]
    yesterday = datetime.now() - timedelta(days=1)
    # Build the common query parameters that apply for each location.
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
        ('dailyRange.start_date.month', '04'),
        ('dailyRange.start_date.day', '19'),
        ('dailyRange.end_date.year', '2025'),
        ('dailyRange.end_date.month', '04'),
        ('dailyRange.end_date.day', '22')
    ]
    
    # Container for all rows across locations.
    all_rows = []
    
    # Process each location.
    for loc_id in location_ids:
        # Build the endpoint URL using the current location id.
        location_path = f"locations/{loc_id}"
        endpoint = f"{base_url}/{location_path}:fetchMultiDailyMetricsTimeSeries"
        
        # Make the GET request.
        response = authed_session.get(endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
        else:
            print(f"Error for location {loc_id}: {response.status_code} {response.text}")
            continue  # Proceed to the next location if one fails.
    
        # Transform the JSON data into rows, adding a profile_id column.
        # Instead of using a dictionary keyed solely by date (which could mix dates across locations), use a list.
        # Each row is a dict with 'date', 'profile_id', and the metric values.
        metric_names = []
        time_series_list = data.get("multiDailyMetricTimeSeries", [])
        if time_series_list:
            # Save the metric names from the first location's first time series entry
            for metric in time_series_list[0].get("dailyMetricTimeSeries", []):
                metric_name = metric.get("dailyMetric")
                metric_names.append(metric_name)
                
            # Process each metric time series into individual rows.
            for metric in time_series_list[0].get("dailyMetricTimeSeries", []):
                metric_name = metric.get("dailyMetric")
                for entry in metric.get("timeSeries", {}).get("datedValues", []):
                    date_info = entry.get("date")
                    date_str = f"{date_info['year']}-{date_info['month']:02d}-{date_info['day']:02d}"
                    
                    # Find if a row for this date and location already exists.
                    # If not, create it.
                    row = next((r for r in all_rows if r.get("date") == date_str and r.get("profile_id") == loc_id), None)
                    if not row:
                        row = {'date': date_str, 'profile_id': loc_id}
                        all_rows.append(row)
                    
                    # Add/overwrite the metric value
                    if "value" in entry:
                        try:
                            value = int(entry["value"])
                        except ValueError:
                            value = entry["value"]
                        row[metric_name] = value
                    else:
                        row[metric_name] = None
        else:
            print(f"No time series data for location {loc_id}.")
    print(all_rows)
    # Initialize BigQuery client.
    # bq_client = bigquery.Client()
    # project = bq_client.project
    # dataset_id = 'GOOGLE_BUSINESS_PROFILE'
    # table_id = 'performance_metrics'
    # dataset_ref = bigquery.DatasetReference(project, dataset_id)

    # # Create the dataset if it doesn't exist.
    # try:
    #     bq_client.get_dataset(dataset_ref)
    #     print(f"Dataset {dataset_id} already exists.")
    # except NotFound:
    #     dataset = bigquery.Dataset(dataset_ref)
    #     dataset = bq_client.create_dataset(dataset)
    #     print(f"Created dataset {dataset_id}.")

    # table_ref = dataset_ref.table(table_id)

    # # Define the table schema: DATE, STRING for profile_id, plus one INTEGER column per metric.
    # schema = [
    #     bigquery.SchemaField("date", "DATE"),
    #     bigquery.SchemaField("profile_id", "STRING")
    # ]
    # unique_metrics = sorted(set(metric_names))
    # for metric in unique_metrics:
    #     schema.append(bigquery.SchemaField(metric, "INTEGER", mode="NULLABLE"))

    # # Create the table if it doesn't exist.
    # try:
    #     bq_client.get_table(table_ref)
    #     print(f"Table {table_id} already exists.")
    # except NotFound:
    #     table = bigquery.Table(table_ref, schema=schema)
    #     table = bq_client.create_table(table)
    #     print(f"Created table {table_id}.")

    # # Insert the rows into the table.
    # errors = bq_client.insert_rows_json(table_ref, all_rows)
    # if errors:
    #     print("Encountered errors while inserting rows:", errors)
    # else:
    #     print("Data inserted successfully.")

if __name__ == '__main__':
    main()
