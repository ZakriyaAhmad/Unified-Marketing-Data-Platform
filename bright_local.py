import time
import requests
import os
from datetime import datetime
from google.cloud import bigquery

# Set your Google Cloud credentials (if not already set in your environment)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

# BrightLocal API settings
API_KEY = ''
BASE_URL = 'https://tools.brightlocal.com/seo-tools/api/v4'
BATCH_URL = f'{BASE_URL}/batch'
FETCH_REVIEWS_URL = f'{BASE_URL}/ld/fetch-reviews'

# BigQuery dataset and table names
DATASET_ID = ''
SUMMARY_TABLE = ''
DETAILED_TABLE = ''

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
            return None
    else:
        print('Error creating batch:', response.status_code, response.text)
        return None

def fetch_reviews(api_key, batch_id):
    payload = {
        'batch-id': batch_id,
        'api-key': api_key,
        "profile-url": "",
        "country": "USA"
    }
    response = requests.post(FETCH_REVIEWS_URL, data=payload)
    if response.status_code == 201:
        data = response.json()
        if data.get('success'):
            return data.get('job-id')
        else:
            print('Failed to fetch reviews:', data)
            return None
    else:
        print('Error fetching reviews:', response.status_code, response.text)
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
    payload = {'batch-id': batch_id, 'api-key': api_key}
    response = requests.get(BATCH_URL, params=payload)
    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            results = data.get('results', {})
            ld_fetch_reviews = results.get('LdFetchReviews', [])
            if ld_fetch_reviews:
                status = ld_fetch_reviews[0].get('status')
                if status == 'Completed':
                    results_container = ld_fetch_reviews[0].get("results", [])
                    if not results_container:
                        print("No results container found.")
                        return None

                    reviews = results_container[0].get("reviews", [])
                    if not reviews:
                        print("No reviews found.")
                        return None

                    # Calculate summary statistics
                    total_reviews = len(reviews)
                    sum_ratings = sum(review.get("rating", 0) for review in reviews if review.get("rating") is not None)
                    avg_rating = sum_ratings / total_reviews if total_reviews > 0 else 0

                    # Build count per rating (assuming ratings 1-5)
                    reviews_by_rating = {}
                    for review in reviews:
                        rating = review.get("rating")
                        if rating is not None:
                            reviews_by_rating[rating] = reviews_by_rating.get(rating, 0) + 1
                    print(reviews_by_rating)
                    summary_data = {
                        "total_reviews": total_reviews,
                        "average_rating": avg_rating,
                        "rating_0": reviews_by_rating.get(0, 0),
                        "rating_1": reviews_by_rating.get(1, 0),
                        "rating_2": reviews_by_rating.get(2, 0),
                        "rating_3": reviews_by_rating.get(3, 0),
                        "rating_4": reviews_by_rating.get(4, 0),
                        "rating_5": reviews_by_rating.get(5, 0),
                        "batch_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    }

                    print("Review Summary:")
                    print("-" * 50)
                    print(f"Total Reviews: {total_reviews}")
                    print(f"Average Rating: {avg_rating:.2f}")
                    for rating in sorted(reviews_by_rating, reverse=True):
                        print(f"  Rating {rating}: {reviews_by_rating[rating]} review(s)")
                    print("-" * 50)
                    print("Batch processing completed.")
                    return reviews, summary_data
                else:
                    print('Batch is still processing. Current status:', status)
                    return None
            else:
                print('No results found in the batch response.')
                return None
        else:
            print('Failed to retrieve batch status:', data)
            return None
    else:
        print('Error checking batch status:', response.status_code, response.text)
        return None

# ---- Data Cleaning Function ----

def clean_review_data(review):
    """
    Transforms the review record to match the BigQuery table schema:
      - Ensures 'timestamp' is in ISO 8601 format (if only a date is provided, appends time)
      - Maps keys to match the schema: author, rating, timestamp, text, rid, author_avatar.
    """
    cleaned = {}
    # Map values with defaults
    cleaned["author"] = review.get("author", "Unknown")
    try:
        # Ensure rating is a float
        cleaned["rating"] = float(review.get("rating", 0))
    except (ValueError, TypeError):
        cleaned["rating"] = 0.0

    # Convert timestamp to ISO format if needed
    ts = review.get("timestamp")
    if ts:
        # Check if already in ISO format (contains 'T')
        if "T" not in ts:
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d")
                ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                ts = None
        cleaned["timestamp"] = ts
    else:
        cleaned["timestamp"] = None

    # Map review text, review id, and avatar URL to match the schema
    cleaned["text"] = review.get("text") or "No review text provided"
    cleaned["rid"] = review.get("rid") or ""
    cleaned["author_avatar"] = review.get("author_avatar") or ""

    return cleaned

# ---- BigQuery Functions ----

def create_dataset_and_tables(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    # Create reviews_summary table
    summary_table_ref = dataset_ref.table(SUMMARY_TABLE)
    try:
        client.get_table(summary_table_ref)
        print(f"Table {SUMMARY_TABLE} already exists in dataset {dataset_id}.")
    except Exception:
        summary_schema = [
            bigquery.SchemaField("total_reviews", "INTEGER"),
            bigquery.SchemaField("average_rating", "FLOAT"),
            bigquery.SchemaField("rating_0", "INTEGER"),
            bigquery.SchemaField("rating_1", "INTEGER"),
            bigquery.SchemaField("rating_2", "INTEGER"),
            bigquery.SchemaField("rating_3", "INTEGER"),
            bigquery.SchemaField("rating_4", "INTEGER"),
            bigquery.SchemaField("rating_5", "INTEGER"),
            bigquery.SchemaField("batch_timestamp", "TIMESTAMP"),
        ]
        summary_table = bigquery.Table(summary_table_ref, schema=summary_schema)
        client.create_table(summary_table)
        print(f"Table {SUMMARY_TABLE} created in dataset {dataset_id}.")

    # Create reviews_detailed table
    detailed_table_ref = dataset_ref.table(DETAILED_TABLE)
    try:
        client.get_table(detailed_table_ref)
        print(f"Table {DETAILED_TABLE} already exists in dataset {dataset_id}.")
    except Exception:
        detailed_schema = [
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("rating", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("rid", "STRING"),
            bigquery.SchemaField("author_avatar", "STRING"),
        ]
        detailed_table = bigquery.Table(detailed_table_ref, schema=detailed_schema)
        client.create_table(detailed_table)
        print(f"Table {DETAILED_TABLE} created in dataset {dataset_id}.")

def load_reviews_summary_into_bigquery(client, summary_data, dataset_id, table_name):
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    load_job = client.load_table_from_json([summary_data], table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete
    if load_job.errors:
        print("Errors while inserting summary data:", load_job.errors)
    else:
        print("Summary data loaded successfully into BigQuery.")

def load_reviews_detailed_into_bigquery(client, reviews, dataset_id, table_name):
    table_ref = client.dataset(dataset_id).table(table_name)
    cleaned_reviews = [clean_review_data(review) for review in reviews]
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    load_job = client.load_table_from_json(cleaned_reviews, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete
    if load_job.errors:
        print("Errors while inserting detailed reviews:", load_job.errors)
    else:
        print("Detailed reviews loaded successfully into BigQuery.")


# ---- Main Orchestration ----

def main():
    client = bigquery.Client()
    
    # Create dataset and both tables if they don't exist
    create_dataset_and_tables(client, DATASET_ID)

    # Step 1: Create a new batch and get the batch ID from BrightLocal
    batch_id = create_batch(API_KEY)
    if not batch_id:
        return

    # Step 2: Fetch reviews using the BrightLocal API
    job_id = fetch_reviews(API_KEY, batch_id)
    if not job_id:
        return

    # Step 3: Commit the batch for processing
    commit_batch(API_KEY, batch_id)

    # Step 4: Poll for batch status until reviews are ready
    result = None
    while result is None:
        time.sleep(60)  # Wait 60 seconds before checking again
        result = check_batch_status(API_KEY, batch_id)
    
    # Unpack detailed reviews and summary data
    reviews, summary_data = result

    # Step 5: Load summary and detailed review data into BigQuery
    load_reviews_summary_into_bigquery(client, summary_data, DATASET_ID, SUMMARY_TABLE)
    load_reviews_detailed_into_bigquery(client, reviews, DATASET_ID, DETAILED_TABLE)

if __name__ == '__main__':
    main()
