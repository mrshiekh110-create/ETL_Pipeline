# Currency Exchange ETL Pipeline with Astro CLI & MongoDB

Production-grade ETL pipeline developed with **Astronomer CLI (Astro CLI)** and **Apache Airflow** to automate daily currency exchange rate extraction from the public **Frankfurter API**, schema standardization, and loading into **MongoDB** with **Mongo Express** web administration.

---

## Architecture Overview
The pipeline consists of three sequential tasks communicating strictly via **Airflow Named XCom**:
1. **`extract_currency_exchange_rates`**: Fetches daily exchange rate rates for `EUR` against `USD, GBP, JPY` from `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`. Pushes to Named XCom key `raw_currency_data`.
2. **`transform_exchange_rate_data`**: Pulls from `raw_currency_data`, standardizes the format, adds metadata (`source`, `retrieved_at`), and pushes to Named XCom key `transformed_currency_data`.
3. **`load_data_into_mongodb`**: Pulls from `transformed_currency_data`, connects to MongoDB, and upserts into `currency_exchange.exchange_rates`.

---

## Prerequisites
- [Docker Desktop](https://www.docker.com/) (running)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`astro`)

---

## Getting Started

1. **Clone the repository:**
   ```bash
   git clone <REPO_URL>
   cd "task 3 pipeline"
   ```

2. **Verify DAG integrity:**
   ```bash
   astro dev parse
   ```

3. **Start the local Airflow + MongoDB + Mongo Express environment:**
   ```bash
   astro dev start
   ```

4. **Access the Web Interfaces:**
   - **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Default user: `admin` / `admin`)
   - **Mongo Express UI**: [http://localhost:8081](http://localhost:8081) (Basic Auth: `admin` / `pass`)
   - **MongoDB Direct Port**: `localhost:27017` (`admin` / `password123`)

5. **Trigger DAG execution:**
   ```bash
   astro dev run dags trigger currency_exchange_etl_pipeline
   ```
   Or run an immediate in-process test:
   ```bash
   astro dev run dags test currency_exchange_etl_pipeline
   ```

---

## Transformed Data Schema
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

---

## Infrastructure Configuration
- `docker-compose.override.yml`: Defines `mongodb` (persistent volume `mongodb_data`) and `mongo-express` connected via the shared `airflow` network.
- `requirements.txt`: Includes `pymongo>=4.6.0` and `requests>=2.31.0`.
- Full execution screenshots and reports are available in [ETL_PIPELINE_REPORT.md](ETL_PIPELINE_REPORT.md).
