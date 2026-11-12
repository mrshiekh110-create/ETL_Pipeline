"""
Currency Exchange ETL Pipeline DAG
===================================
Automated ETL pipeline using Astro CLI and Apache Airflow to extract daily currency exchange rates
from the Frankfurter API (https://api.frankfurter.dev), transform the data into a standardized
schema, and load it into a MongoDB collection with Mongo Express monitoring.

Tasks:
  1. extract_currency_exchange_rates: Fetches raw exchange rate data from Frankfurter API.
  2. transform_exchange_rate_data: Standardizes schema and adds metadata.
  3. load_data_into_mongodb: Inserts/upserts records into MongoDB.

Airflow Named XCom is used for transferring data between tasks.
"""

from datetime import UTC, datetime, timedelta
import logging
import os
import pymongo
import requests
from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# Configuration constants
API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_DB_NAME = os.getenv("MONGO_DB", "currency_exchange")
MONGO_COLLECTION_NAME = "exchange_rates"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def extract_currency_exchange_rates(ti, **kwargs):
    """
    Task 1: Extract Currency Exchange Rates
    Fetches daily exchange rate data for EUR to USD, GBP, JPY from Frankfurter API.
    Pushes raw response to Airflow XCom under the named key 'raw_currency_data'.
    """
    logger.info("Connecting to Frankfurter API: %s", API_URL)
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    raw_data = response.json()
    logger.info("Successfully fetched %d rate records from Frankfurter API", len(raw_data))
    logger.info("Raw API Data: %s", raw_data)

    # Named XCom push
    ti.xcom_push(key="raw_currency_data", value=raw_data)
    logger.info("Successfully pushed raw currency data to named XCom key 'raw_currency_data'")


def transform_exchange_rate_data(ti, **kwargs):
    """
    Task 2: Transform Exchange Rate Data
    Pulls raw currency data from XCom key 'raw_currency_data', parses the rates,
    and standardizes them into the required document schema:
    {
      "base_currency": "EUR",
      "target_currency": "USD",
      "exchange_rate": 1.15,
      "rate_date": "2026-11-04",
      "source": "Frankfurter API",
      "retrieved_at": "2026-11-05T07:15:00"
    }
    Pushes transformed data to Airflow XCom under the named key 'transformed_currency_data'.
    """
    # Named XCom pull
    raw_data = ti.xcom_pull(key="raw_currency_data", task_ids="extract_currency_exchange_rates")
    if not raw_data:
        raise ValueError("No raw currency data received from extract task")

    retrieved_at_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records = []

    # Handle list of rate objects returned by Frankfurter v2 API
    # e.g., [{"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1186}, ...]
    for item in raw_data:
        record = {
            "base_currency": str(item.get("base")),
            "target_currency": str(item.get("quote")),
            "exchange_rate": float(item.get("rate")),
            "rate_date": str(item.get("date")),
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at_iso,
        }
        transformed_records.append(record)
        logger.info("Transformed record: %s", record)

    logger.info("Total transformed records: %d", len(transformed_records))

    # Named XCom push
    ti.xcom_push(key="transformed_currency_data", value=transformed_records)
    logger.info("Successfully pushed transformed data to named XCom key 'transformed_currency_data'")


def load_data_into_mongodb(ti, **kwargs):
    """
    Task 3: Load Data into MongoDB
    Pulls transformed data from XCom key 'transformed_currency_data' and
    inserts/upserts each record into the MongoDB collection 'exchange_rates'.
    """
    # Named XCom pull
    transformed_records = ti.xcom_pull(
        key="transformed_currency_data", task_ids="transform_exchange_rate_data"
    )
    if not transformed_records:
        raise ValueError("No transformed records found to load into MongoDB")

    # Connect to MongoDB with fallback for local testing
    connection_uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        f"{MONGO_DB_NAME}?authSource=admin"
    )
    logger.info("Connecting to MongoDB at %s:%d (Database: %s)", MONGO_HOST, MONGO_PORT, MONGO_DB_NAME)

    client = pymongo.MongoClient(connection_uri, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    upserted_count = 0
    for record in transformed_records:
        filter_query = {
            "base_currency": record["base_currency"],
            "target_currency": record["target_currency"],
            "rate_date": record["rate_date"],
        }
        result = collection.update_one(filter_query, {"$set": record}, upsert=True)
        if result.upserted_id or result.modified_count > 0:
            upserted_count += 1
        logger.info(
            "Processed record: %s -> %s on %s (upserted_id=%s, modified=%s)",
            record["base_currency"],
            record["target_currency"],
            record["rate_date"],
            result.upserted_id,
            result.modified_count,
        )

    total_in_db = collection.count_documents({})
    logger.info(
        "Successfully loaded %d records. Total documents in '%s': %d",
        len(transformed_records),
        MONGO_COLLECTION_NAME,
        total_in_db,
    )

    # Print sample document for validation
    sample = collection.find_one({"source": "Frankfurter API"})
    logger.info("Sample MongoDB Document: %s", sample)
    client.close()


with DAG(
    dag_id="currency_exchange_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline integrating Frankfurter daily currency exchange rates to MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["currency", "etl", "frankfurter", "mongodb"],
) as dag:

    # Task 1: Extract Currency Exchange Rates
    extract_task = PythonOperator(
        task_id="extract_currency_exchange_rates",
        python_callable=extract_currency_exchange_rates,
    )

    # Task 2: Transform Exchange Rate Data
    transform_task = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_exchange_rate_data,
    )

    # Task 3: Load Data into MongoDB
    load_task = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_data_into_mongodb,
    )

    # Task pipeline dependency
    extract_task >> transform_task >> load_task

