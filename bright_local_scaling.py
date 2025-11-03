import time
import requests
import os
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from google.cloud import bigquery

# Set your Google Cloud credentials (if not already set in your environment)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

# BrightLocal API settings
API_KEY = ''
BASE_URL = 'https://tools.brightlocal.com/seo-tools/api/v4'
BATCH_URL = f'{BASE_URL}/batch'
FETCH_REVIEWS_URL = f'{BASE_URL}/ld/fetch-reviews'

# BigQuery dataset and table name
DATASET_ID = ''
DETAILED_TABLE = ''

# List of profile IDs (place IDs) to process
profile_ids = [
]

# ---- BrightLocal API Functions ----

def create_batch(api_key):
    payload = {'api-key': api_key}
    response = requests.post(BATCH_URL, data=payload)
    if response.status_code == 201:
        data = response.json()
        if data.get('success'):
            return data.get('batch-id')
        else:
            print('Failed to create batch:', data)
    else:
        print('Error creating batch:', response.status_code, response.text)
    return None

def fetch_reviews(api_key, batch_id, profile_id):
    profile_url = f"https://search.google.com/local/writereview?placeid={profile_id}"
    payload = {
        'batch-id': batch_id,
        'api-key': api_key,
        "profile-url": profile_url,
        "reviews-limit": "all",
        "country": "USA"
    }
    response = requests.post(FETCH_REVIEWS_URL, data=payload)
    if response.status_code == 201:
        data = response.json()
        if data.get('success'):
            return data.get('job-id')
        else:
            print(f'Failed to fetch reviews for place id {profile_id}:', data)
    else:
        print(f'Error fetching reviews for place id {profile_id}:', response.status_code, response.text)
    return None

def commit_batch(api_key, batch_id):
    payload = {'batch-id': batch_id, 'api-key': api_key}
    response = requests.put(BATCH_URL, data=payload)
    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print('Batch committed successfully.')
        else:
            print('Failed to commit batch:', data)
    else:
        print('Error committing batch:', response.status_code, response.text)

def check_batch_status(api_key, batch_id):
    """
    Checks the batch status and makes sure every job is completed.
    Only when all jobs are marked as "Completed", the function aggregates
    all review records (each with an extra "place_id" column) and returns them.
    """
    payload = {'batch-id': batch_id, 'api-key': api_key}
    response = requests.get(BATCH_URL, params=payload)
    if response.status_code == 200:
        data = response.json()
        if not data.get('success'):
            print('Failed to retrieve batch status:', data)
            return None

        results = data.get('results', {})
        ld_fetch_reviews = results.get('LdFetchReviews', [])

        # Check if the number of jobs matches the number of profile_ids
        if len(ld_fetch_reviews) < len(profile_ids):
            print("Not all jobs are present in batch status yet.")
            return None

        # Ensure that all jobs are completed
        incomplete_jobs = [job for job in ld_fetch_reviews if job.get('status') != 'Completed']
        if incomplete_jobs:
            pending = {job.get("job-id"): job.get("status") for job in incomplete_jobs}
            print("Waiting for all jobs to complete. Pending jobs:", pending)
            return None

        # If all jobs are complete, aggregate reviews
        all_reviews = []  # List to accumulate reviews from all jobs
        for job in ld_fetch_reviews:
            # Extract place_id from the payload's "profile-url"
            payload_job = job.get('payload', {})
            profile_url = payload_job.get("profile-url", "")
            parsed_url = urlparse(profile_url)
            qs = parse_qs(parsed_url.query)
            place_id = qs.get("placeid", ["Unknown"])[0]

            # Extract reviews from job result
            results_container = job.get("results", [])
            if not results_container:
                print(f"No results container found for place id {place_id}.")
                continue
            reviews_block = results_container[0]
            reviews = reviews_block.get("reviews", [])
            if not reviews:
                print(f"No reviews found for place id {place_id}.")
                continue

            # Tag each review with the corresponding place_id
            for review in reviews:
                review["place_id"] = place_id
            all_reviews.extend(reviews)

        return all_reviews

    else:
        print('Error checking batch status:', response.status_code, response.text)
        return None

# ---- BigQuery Functions ----

def create_dataset_and_table(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    # Create the reviews_detailed table with the expected column data types.
    # The "timestamp" column is now defined as "date".
    detailed_table_ref = dataset_ref.table(DETAILED_TABLE)
    try:
        client.get_table(detailed_table_ref)
        print(f"Table {DETAILED_TABLE} already exists in dataset {dataset_id}.")
    except Exception:
        detailed_schema = [
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("rating", "FLOAT"),
            # The API provides a date (e.g., "2025-04-05"), stored as a DATE type.
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("rid", "STRING"),
            bigquery.SchemaField("author_avatar", "STRING"),
            bigquery.SchemaField("place_id", "STRING")
        ]
        detailed_table = bigquery.Table(detailed_table_ref, schema=detailed_schema)
        client.create_table(detailed_table)
        print(f"Table {DETAILED_TABLE} created in dataset {dataset_id}.")

def load_reviews_detailed_into_bigquery(client, reviews, dataset_id, table_name):
    table_ref = client.dataset(dataset_id).table(table_name)
    
    # Map the "timestamp" field from the API response to "date"
    mapped_reviews = []
    for review in reviews:
        review["date"] = review.get("timestamp")
        if "timestamp" in review:
            del review["timestamp"]
        mapped_reviews.append(review)
    
    # Since the API response is in the desired format, load the mapped reviews directly into BigQuery.
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_json(mapped_reviews, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    if load_job.errors:
        print("Errors while inserting detailed reviews:", load_job.errors)
    else:
        print("Detailed reviews loaded successfully into BigQuery.")

# ---- Main Orchestration ----

def main():
    client = bigquery.Client()
    
    # Create dataset and detailed reviews table if they do not exist
    create_dataset_and_table(client, DATASET_ID)

    # Step 1: Create a new batch
    batch_id = create_batch(API_KEY)
    if not batch_id:
        return

    # Step 2: Submit a review job for each profile ID
    for place_id in profile_ids:
        job_id = fetch_reviews(API_KEY, batch_id, place_id)
        if job_id:
            print(f"Job {job_id} created for place id {place_id}")
        else:
            print(f"Job not created for place id {place_id}")

    # Step 3: Commit the batch for processing
    commit_batch(API_KEY, batch_id)

    # Step 4: Poll until all jobs are completed
    all_reviews = None
    while all_reviews is None:
        time.sleep(60)  # Wait 60 seconds before checking again
        all_reviews = check_batch_status(API_KEY, batch_id)
        if all_reviews is None:
            print("Waiting for all jobs to complete...")

    # Step 5: All jobs are completed, load the detailed reviews into BigQuery
    load_reviews_detailed_into_bigquery(client, all_reviews, DATASET_ID, DETAILED_TABLE)

if __name__ == '__main__':
    main()
