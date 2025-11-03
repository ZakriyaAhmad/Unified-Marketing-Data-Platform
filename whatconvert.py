import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def main():
    # -----------------------
    # Step 1: Retrieve and Process API Data
    # -----------------------
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
    # API endpoint and credentials
    url = ""
    username = ""
    password = ""
    yesterday = datetime.now() - timedelta(days=1)
    params = {
        "start_date": f"2025-04-14T00:00:00Z",
        "leads_per_page": "2500"
    }

    # Perform a GET request using HTTP Basic Authentication
    response = requests.get(url, auth=(username, password), params=params)

    if response.status_code == 200:
        data = response.json()  # Parse the JSON response
        print("Leads retrieved successfully!")
        
        leads = data.get('leads', [])
        if not leads:
            print("No leads found in the response.")
            exit()
        
        # Convert the list of leads to a DataFrame
        df = pd.DataFrame(leads)
        
        # Extract the date from 'date_created' (removing the time)
        df['date'] = pd.to_datetime(df['date_created']).dt.date
        
        # Create columns for counting phone calls and web forms based on lead_type
        # (Assuming that the lead_type field is either "Phone Call" or "Web Form")
        df['phone_call'] = (df['lead_type'].str.lower() == 'phone call').astype(int)
        df['web_form'] = (df['lead_type'].str.lower() == 'web form').astype(int)
        
        # Group by date, account_id, and account to aggregate counts
        result = df.groupby(['date', 'account_id', 'account'], as_index=False)[['phone_call', 'web_form']].sum()
        
        print("Processed DataFrame:")
        print(result)
    else:
        print(f"Request failed with status code {response.status_code}")
        print("Response:", response.text)
        exit()

    # -----------------------
    # Step 2: Dump the DataFrame into BigQuery
    # -----------------------

    # # Create a BigQuery client
    # client = bigquery.Client()

    # # Replace with your actual GCP project ID if not set in your environment.
    # project_id = client.project

    # # Define dataset and table IDs
    # dataset_id = "WhatConverts"
    # table_id = "Leads"
    # full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # # Create the dataset if it doesn't exist
    # dataset_ref = client.dataset(dataset_id)
    # try:
    #     client.get_dataset(dataset_ref)
    #     print(f"Dataset '{dataset_id}' already exists.")
    # except NotFound:
    #     dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    #     dataset = client.create_dataset(dataset)
    #     print(f"Dataset '{dataset_id}' created.")

    # # Create the table if it doesn't exist with the desired schema
    # table_ref = dataset_ref.table(table_id)
    # try:
    #     client.get_table(table_ref)
    #     print(f"Table '{table_id}' already exists in dataset '{dataset_id}'.")
    # except NotFound:
    #     schema = [
    #         bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    #         bigquery.SchemaField("account_id", "INTEGER", mode="REQUIRED"),
    #         bigquery.SchemaField("account", "STRING", mode="REQUIRED"),
    #         bigquery.SchemaField("phone_call", "INTEGER", mode="NULLABLE"),
    #         bigquery.SchemaField("web_form", "INTEGER", mode="NULLABLE"),
    #     ]
    #     table = bigquery.Table(full_table_id, schema=schema)
    #     table = client.create_table(table)
    #     print(f"Table '{table_id}' created in dataset '{dataset_id}'.")

    # # Load the DataFrame into BigQuery
    # job_config = bigquery.LoadJobConfig(
    #     write_disposition="WRITE_APPEND",  # Appends data to the table
    # )
    # load_job = client.load_table_from_dataframe(result, full_table_id, job_config=job_config)
    # load_job.result()  # Wait for the job to complete

    # print(f"Loaded data into BigQuery table '{full_table_id}'.")
    
if __name__ == '__main__':
    main()