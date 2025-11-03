import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension, # Keep Dimension import even if not used in this version, might be needed later
    Metric,
    RunReportRequest,
)

# --- Configuration ---
# Replace with your GA4 Property ID
# You can find this in your GA4 Admin settings under Property -> Property Settings
PROPERTY_ID = ""

# Replace with the path to your service account key file
# Ensure the service account has read access to your GA4 property
SERVICE_ACCOUNT_KEY_FILE = ""

# Set the environment variable for authentication
# This tells the Google client library where to find the credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_FILE

# --- Function to run the report ---
def run_ga4_total_report(property_id, start_date, end_date):
    """
    Runs a report on a Google Analytics 4 property to fetch total metrics
    for a specified date range.
    """

    # Initialize the client
    # Use the beta client as some features might still be in beta
    client = BetaAnalyticsDataClient()

    # Construct the request
    request = RunReportRequest(
        property=f"properties/{property_id}",
        # No dimensions are included to get total metrics across the date range
        # dimensions=[
        #     Dimension(name="date"), # Removed the date dimension
        # ],
        metrics=[
            # Metric for total sessions
            Metric(name="sessions"),
            # Metric for engaged sessions
            Metric(name="engagedSessions"),
            # Metric for total event count
            Metric(name="eventCount"),
            # Metric for key events (conversions)
            Metric(name="conversions"), # Note: 'conversions' is the API name for Key Events
        ],
        date_ranges=[
            # Define the specific date range for the report
            DateRange(start_date=start_date, end_date=end_date),
        ],
        # Add filters or segments if needed
        # For example, to filter by a specific channel group:
        # dimension_filter=FilterExpression(
        #     filter=Filter(
        #         field_name="sessionDefaultChannelGroup",
        #         string_filter=Filter.StringFilter(value="Organic Search"),
        #     )
        # ),
    )

    # Run the report and get the response
    response = client.run_report(request)

    # --- Process and print the results ---
    print(f"Report response for date range: {start_date} to {end_date}")
    print(f"Row count: {len(response.rows)}") # Should be 1 row for total data
    print(f"Column header: {response.metric_headers}") # Only metric headers

    if response.rows:
        # There should be only one row containing the totals
        total_row = response.rows[0]
        sessions_total = total_row.metric_values[0].value
        engaged_sessions_total = total_row.metric_values[1].value
        event_count_total = total_row.metric_values[2].value
        conversions_total = total_row.metric_values[3].value

        print("--- Totals ---")
        print(f"Total Sessions: {sessions_total}")
        print(f"Total Engaged Sessions: {engaged_sessions_total}")
        print(f"Total Event Count: {event_count_total}")
        print(f"Total Key Events (Conversions): {conversions_total}")
    else:
        print("No data found for the specified date range.")


# --- Main execution block ---
if __name__ == "__main__":
    # Check if the key file exists
    if not os.path.exists(SERVICE_ACCOUNT_KEY_FILE):
        print(f"Error: Service account key file not found at {SERVICE_ACCOUNT_KEY_FILE}")
        print("Please update the SERVICE_ACCOUNT_KEY_FILE path in the script.")
    else:
        # --- Specify the date range ---
        # Replace with your desired start and end dates in 'YYYY-MM-DD' format
        report_start_date = "2025-04-17" # Example start date
        report_end_date = "2025-04-23"   # Example end date

        # Run the report to get totals
        run_ga4_total_report(PROPERTY_ID, report_start_date, report_end_date)

