# Building Airflow ETL Pipeline: Frankfurter API to MongoDB Integration

## Executive Summary
This report documents the implementation and verification of an Apache Airflow ETL pipeline built using Astronomer CLI (`astro`). The pipeline extracts daily currency exchange rates from the public **Frankfurter API**, standardizes and transforms the exchange rate records, and loads them into **MongoDB** with **Mongo Express** configured as the web administration interface. All inter-task communications strictly utilize **Airflow (Named) XCom**.

---

## 1. Architecture & System Design

```
+-----------------------------------------------------------------------------------+
|                              Apache Airflow (Astro CLI)                           |
|                                                                                   |
|  +----------------------------------+                                             |
|  | Task 1:                          |                                             |
|  | extract_currency_exchange_rates  |                                             |
|  | (GET https://api.frankfurter.dev)|                                             |
|  +-----------------+----------------+                                             |
|                    |                                                              |
|                    | Named XCom Push: key="raw_currency_data"                     |
|                    v                                                              |
|  +-----------------+----------------+                                             |
|  | Task 2:                          |                                             |
|  | transform_exchange_rate_data     |                                             |
|  | (Schema standardization)         |                                             |
|  +-----------------+----------------+                                             |
|                    |                                                              |
|                    | Named XCom Push: key="transformed_currency_data"             |
|                    v                                                              |
|  +-----------------+----------------+                                             |
|  | Task 3:                          |                                             |
|  | load_data_into_mongodb           |                                             |
|  | (Upsert into 'exchange_rates')   |                                             |
|  +-----------------+----------------+                                             |
+--------------------|--------------------------------------------------------------+
                     | Docker Bridged Network ('airflow')
                     v
+--------------------+--------------------------------------------------------------+
|                              Docker Services Infrastructure                       |
|                                                                                   |
|  +--------------------------------+       +------------------------------------+  |
|  | MongoDB Primary Database       | <---> | Mongo Express Admin Interface      |  |
|  | Image: mongo:7.0               |       | Image: mongo-express:1.0.2         |  |
|  | Port: 27017                    |       | Port: 8081                         |  |
|  | Volume: mongodb_data (Persist) |       | Auth: admin / pass                 |  |
|  +--------------------------------+       +------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

---

## 2. Docker & Environment Configuration

### Docker Networking & Persistent Storage
In accordance with Astro CLI specifications, custom services are orchestrated in `docker-compose.override.yml`:
- **Docker Networking**: `airflow` network connects the Airflow worker/scheduler containers, MongoDB, and Mongo Express.
- **Persistent Storage**: Docker named volume `mongodb_data` is mounted to `/data/db` on MongoDB to ensure persistence across container restarts.

#### `docker-compose.override.yml`
```yaml
services:
  mongodb:
    image: mongo:7.0
    container_name: mongodb
    restart: unless-stopped
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password123
      MONGO_INITDB_DATABASE: currency_exchange
    volumes:
      - mongodb_data:/data/db
    networks:
      airflow:
        aliases:
          - mongo
          - mongodb

  mongo-express:
    image: mongo-express:1.0.2-20-alpine3.19
    container_name: mongo-express
    restart: unless-stopped
    ports:
      - "8081:8081"
    environment:
      ME_CONFIG_MONGODB_URL: mongodb://admin:password123@mongodb:27017/?authSource=admin
      ME_CONFIG_MONGODB_ADMINUSERNAME: admin
      ME_CONFIG_MONGODB_ADMINPASSWORD: password123
      ME_CONFIG_MONGODB_SERVER: mongodb
      ME_CONFIG_MONGODB_PORT: 27017
      ME_CONFIG_BASICAUTH_USERNAME: admin
      ME_CONFIG_BASICAUTH_PASSWORD: pass
    depends_on:
      - mongodb
    networks:
      - airflow

volumes:
  mongodb_data:
    driver: local

networks:
  airflow:
```

#### `requirements.txt`
```
pymongo>=4.6.0
requests>=2.31.0
```

---

## 3. Airflow DAG Implementation

**DAG ID**: `currency_exchange_etl_pipeline`  
**File**: `dags/currency_exchange_etl_dag.py`  
**Schedule**: `@daily`

### 3.1 Task 1: Extract Currency Exchange Rates
- Endpoint: `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`
- Pushes fetched JSON payload via **Named XCom**:
  ```python
  ti.xcom_push(key="raw_currency_data", value=raw_data)
  ```

### 3.2 Task 2: Transform Exchange Rate Data
- Pulls from **Named XCom**:
  ```python
  raw_data = ti.xcom_pull(key="raw_currency_data", task_ids="extract_currency_exchange_rates")
  ```
- Normalizes data into the required document schema:
  ```json
  {
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.1186,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T04:17:10"
  }
  ```
- Pushes standardized list via **Named XCom**:
  ```python
  ti.xcom_push(key="transformed_currency_data", value=transformed_records)
  ```

### 3.3 Task 3: Load Data into MongoDB
- Pulls from **Named XCom**:
  ```python
  transformed_records = ti.xcom_pull(
      key="transformed_currency_data", task_ids="transform_exchange_rate_data"
  )
  ```
- Connects to `mongodb://admin:password123@mongodb:27017/currency_exchange?authSource=admin`.
- Upserts documents into collection `exchange_rates` on the unique composite key `(base_currency, target_currency, rate_date)` to ensure idempotency.

---

## 4. Pipeline Execution & Verification Outputs

### 4.1 Astro CLI DAG Verification
The DAG was validated using `astro dev parse`:
```text
======================== 1 passed, 2 warnings in 13.80s ========================
✔ No errors detected in your Dags
```

### 4.2 Astro Task Execution Logs
```text
2026-11-12T04:17:00.927815Z [info] [DAG TEST] starting task_id=extract_currency_exchange_rates
2026-11-12T04:17:01.359281Z [info] Connecting to Frankfurter API: https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY
2026-11-12T04:17:01.621450Z [info] Successfully fetched 3 rate records from Frankfurter API
2026-11-12T04:17:01.622110Z [info] Successfully pushed raw currency data to named XCom key 'raw_currency_data'

2026-11-12T04:17:10.741029Z [info] [DAG TEST] starting task_id=transform_exchange_rate_data
2026-11-12T04:17:10.892401Z [info] Transformed record: {'base_currency': 'EUR', 'target_currency': 'GBP', 'exchange_rate': 0.83644, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
2026-11-12T04:17:10.894102Z [info] Transformed record: {'base_currency': 'EUR', 'target_currency': 'JPY', 'exchange_rate': 177.52, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
2026-11-12T04:17:10.895820Z [info] Transformed record: {'base_currency': 'EUR', 'target_currency': 'USD', 'exchange_rate': 1.1178, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
2026-11-12T04:17:10.897100Z [info] Successfully pushed transformed data to named XCom key 'transformed_currency_data'

2026-11-12T04:17:10.991063Z [info] [DAG TEST] starting task_id=load_data_into_mongodb
2026-11-12T04:17:11.208766Z [info] Connecting to MongoDB at mongodb:27017 (Database: currency_exchange)
2026-11-12T04:17:11.390053Z [info] Processed record: EUR -> GBP on 2026-11-12
2026-11-12T04:17:11.391941Z [info] Processed record: EUR -> JPY on 2026-11-12
2026-11-12T04:17:11.393766Z [info] Processed record: EUR -> USD on 2026-11-12
2026-11-12T04:17:11.395742Z [info] Successfully loaded 3 records. Total documents in 'exchange_rates': 5
2026-11-12T04:17:11.459704Z [info] Marking run manual__2026-11-12T04:17:00.635639+00:00 successful
2026-11-12T04:17:11.462174Z [info] DagRun Finished: state=success
```

### 4.3 Verified MongoDB Stored Documents
```json
[
  {
    "base_currency": "EUR",
    "target_currency": "GBP",
    "exchange_rate": 0.83644,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T04:17:10"
  },
  {
    "base_currency": "EUR",
    "target_currency": "JPY",
    "exchange_rate": 177.52,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T04:17:10"
  },
  {
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.1178,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T04:17:10"
  }
]
```

---

## 5. UI Screenshots

### Screenshot 1: Mongo Express - Home View
![Mongo Express Home View](screenshots/01_mongo_express_home.png)

### Screenshot 2: Mongo Express - Database View (`currency_exchange`)
![Mongo Express Database View](screenshots/02_mongo_express_database.png)

### Screenshot 3: Mongo Express - Stored Exchange Rate Documents
![Mongo Express Documents View](screenshots/03_mongo_express_documents.png)

### Screenshot 4: Apache Airflow UI - DAG Listing & Status
![Airflow UI Home](screenshots/04_airflow_ui_home.png)

### Screenshot 5: Apache Airflow - DAG Details
![Airflow DAG Details](screenshots/05_airflow_dag_details.png)

### Screenshot 6: Apache Airflow - DAG Task Flow Graph
![Airflow DAG Graph](screenshots/06_airflow_dag_graph.png)

---

## 6. GitHub / DagsHub Repository Instructions

The local Git repository is initialized with the `main` branch and contains all source code, tests, compose configurations, and outputs:
- **Commit**: `feat: complete Airflow ETL pipeline with Astro CLI, Frankfurter API, and MongoDB integration`

To push to your remote repository (GitHub or DagsHub), execute:

```bash
# 1. Add your remote repository URL:
git remote add origin https://github.com/<your-username>/<your-repo-name>.git
# (Or for DagsHub: https://dagshub.com/<your-username>/<your-repo-name>.git)

# 2. Push to main:
git branch -M main
git push -u origin main
```
