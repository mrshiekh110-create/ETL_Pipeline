"""
Test script for verifying Frankfurter API extraction, transformation schema,
and loading into MongoDB container.
"""

from datetime import datetime, timezone
import json
import pymongo
import requests

def run_test():
    api_url = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
    print(f"[1] Extracting from: {api_url}")
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    raw_data = response.json()
    print(f"Extracted {len(raw_data)} rate items:")
    print(json.dumps(raw_data, indent=2))

    print("\n[2] Transforming data...")
    retrieved_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records = []
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
    
    print(f"Transformed {len(transformed_records)} records:")
    print(json.dumps(transformed_records, indent=2))

    print("\n[3] Loading into MongoDB at localhost:27017...")
    connection_uri = "mongodb://admin:password123@localhost:27017/?authSource=admin"
    client = pymongo.MongoClient(connection_uri, serverSelectionTimeoutMS=5000)
    db = client["currency_exchange"]
    collection = db["exchange_rates"]

    for record in transformed_records:
        filter_query = {
            "base_currency": record["base_currency"],
            "target_currency": record["target_currency"],
            "rate_date": record["rate_date"],
        }
        result = collection.update_one(filter_query, {"$set": record}, upsert=True)
        print(f"Upserted {record['base_currency']} -> {record['target_currency']}: upserted_id={result.upserted_id}, modified={result.modified_count}")

    count = collection.count_documents({})
    print(f"\nTotal documents in currency_exchange.exchange_rates: {count}")
    print("\nSample document from MongoDB:")
    for doc in collection.find().limit(3):
        # Convert ObjectId to string for JSON printing
        doc["_id"] = str(doc["_id"])
        print(json.dumps(doc, indent=2))

    client.close()
    print("\nETL test completed successfully!")

if __name__ == "__main__":
    run_test()

