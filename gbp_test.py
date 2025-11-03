import json
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request, AuthorizedSession

SCOPES = ['https://www.googleapis.com/auth/business.manage']

def main():
    creds = None
    # Load credentials if available.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If no valid credentials, start the OAuth flow.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # Create an authorized session.
    authed_session = AuthorizedSession(creds)

    # Build the endpoint URL.
    base_url = "https://mybusiness.googleapis.com/v4"
    location_id = ""  # Replace with your location ID
    accounts = ""
    endpoint = f"{base_url}/{accounts}/{location_id}/reviews"

    # Build the query parameters.
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
        ('dailyRange.start_date.year', '2025'),
        ('dailyRange.start_date.month', '1'),
        ('dailyRange.start_date.day', '1'),
        ('dailyRange.end_date.year', '2025'),
        ('dailyRange.end_date.month', '1'),
        ('dailyRange.end_date.day', '5')
    ]
    
    # Make the GET request.
    response = authed_session.get(endpoint)
    if response.status_code == 200:
        data = response.json()

        # Pretty-print the entire JSON response.
        print("Full JSON Response:\n")
        print(json.dumps(data, indent=2))
    else:
        print("Error:", response.status_code, response.text)

if __name__ == '__main__':
    main()
