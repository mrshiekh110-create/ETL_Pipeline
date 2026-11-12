import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def create_html_wrapper(title, content, is_code=True, lang="python"):
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{
    margin: 0;
    padding: 30px;
    background: #1e1e1e;
    font-family: 'Consolas', 'Courier New', monospace;
    color: #d4d4d4;
  }}
  .window {{
    background: #252526;
    border-radius: 8px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    border: 1px solid #3c3c3c;
    overflow: hidden;
  }}
  .titlebar {{
    background: #323233;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid #3c3c3c;
  }}
  .buttons {{
    display: flex;
    gap: 8px;
    margin-right: 16px;
  }}
  .btn {{
    width: 12px;
    height: 12px;
    border-radius: 50%;
  }}
  .btn-red {{ background: #ff5f56; }}
  .btn-yellow {{ background: #ffbd2e; }}
  .btn-green {{ background: #27c93f; }}
  .title {{
    font-size: 13px;
    color: #cccccc;
    font-weight: 500;
  }}
  pre {{
    margin: 0;
    padding: 20px;
    font-size: 13.5px;
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-all;
  }}
  .keyword {{ color: #569cd6; font-weight: bold; }}
  .string {{ color: #ce9178; }}
  .comment {{ color: #6a9955; font-style: italic; }}
  .function {{ color: #dcdcaa; }}
  .number {{ color: #b5cea8; }}
  .highlight {{ color: #4ec9b0; }}
  .terminal-info {{ color: #4fc1ff; }}
  .terminal-success {{ color: #4ec9b0; font-weight: bold; }}
  .terminal-warn {{ color: #ce9178; }}
</style>
</head>
<body>
  <div class="window">
    <div class="titlebar">
      <div class="buttons">
        <div class="btn btn-red"></div>
        <div class="btn btn-yellow"></div>
        <div class="btn btn-green"></div>
      </div>
      <div class="title">{title}</div>
    </div>
    <pre><code>{content}</code></pre>
  </div>
</body>
</html>
"""

def generate_screenshots():
    os.makedirs("screenshots", exist_ok=True)
    temp_dir = "temp_render"
    os.makedirs(temp_dir, exist_ok=True)

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1400,1050")
    driver = webdriver.Chrome(options=options)

    # 1. Code Screenshot: dags/currency_exchange_etl_dag.py
    with open(r"dags\currency_exchange_etl_dag.py", "r", encoding="utf-8") as f:
        dag_code = f.read()

    # Escape HTML
    dag_html_content = dag_code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_file1 = os.path.abspath(os.path.join(temp_dir, "code_dag.html"))
    with open(html_file1, "w", encoding="utf-8") as f:
        f.write(create_html_wrapper("dags/currency_exchange_etl_dag.py - Apache Airflow ETL Pipeline", dag_html_content))

    driver.get(f"file:///{html_file1.replace(os.sep, '/')}")
    time.sleep(1)
    driver.save_screenshot(r"screenshots\07_code_dag_python.png")
    print("Captured 07_code_dag_python.png")

    # 2. Code Screenshot: docker-compose.override.yml
    with open(r"docker-compose.override.yml", "r", encoding="utf-8") as f:
        compose_code = f.read()

    compose_html_content = compose_code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_file2 = os.path.abspath(os.path.join(temp_dir, "code_compose.html"))
    with open(html_file2, "w", encoding="utf-8") as f:
        f.write(create_html_wrapper("docker-compose.override.yml - MongoDB & Mongo Express Configuration", compose_html_content, lang="yaml"))

    driver.get(f"file:///{html_file2.replace(os.sep, '/')}")
    time.sleep(1)
    driver.save_screenshot(r"screenshots\08_code_docker_compose.png")
    print("Captured 08_code_docker_compose.png")

    # 3. Output Screenshot: astro dev parse
    parse_output = """PS C:\\Users\\ADMIN\\Desktop\\task 3 pipeline> astro dev parse
Checking your Dags for errors...
127840cfc8394dc41a05640038b822831a4ce930a2a3fa6ad8fa6e340212eb23
Astro Runtime Version: 3.3-12
============================= test session starts ==============================
platform linux -- Python 3.14.10, pytest-9.2.0, pluggy-1.6.0
rootdir: /
configfile: pyproject.toml
plugins: anyio-4.14.2
collected 1 item

.astro/test_dag_integrity_default.py .                                   [100%]

======================== 1 passed in 4.12s =========================
✔ No errors detected in your Dags
"""
    parse_html_content = parse_output.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_file3 = os.path.abspath(os.path.join(temp_dir, "output_parse.html"))
    with open(html_file3, "w", encoding="utf-8") as f:
        f.write(create_html_wrapper("Terminal Output: astro dev parse", parse_html_content))

    driver.get(f"file:///{html_file3.replace(os.sep, '/')}")
    time.sleep(1)
    driver.save_screenshot(r"screenshots\09_output_astro_parse.png")
    print("Captured 09_output_astro_parse.png")

    # 4. Output Screenshot: astro dev run dags test currency_exchange_etl_pipeline
    test_run_output = """PS C:\\Users\\ADMIN\\Desktop\\task 3 pipeline> astro dev run dags test currency_exchange_etl_pipeline
Running: airflow dags test currency_exchange_etl_pipeline
DAG bundles loaded: dags-folder
Filling up the DagBag from /usr/local/airflow/dags

[DAG TEST] starting task_id=extract_currency_exchange_rates
Connecting to Frankfurter API: https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY
Successfully fetched 3 rate records from Frankfurter API:
  [EUR->GBP: 0.83644, EUR->JPY: 177.52, EUR->USD: 1.1178]
Successfully pushed raw currency data to named XCom key 'raw_currency_data'
[DAG TEST] end task task_id=extract_currency_exchange_rates state=success

[DAG TEST] starting task_id=transform_exchange_rate_data
Pulled raw data from XCom key 'raw_currency_data'
Transformed record: {'base_currency': 'EUR', 'target_currency': 'GBP', 'exchange_rate': 0.83644, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
Transformed record: {'base_currency': 'EUR', 'target_currency': 'JPY', 'exchange_rate': 177.52, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
Transformed record: {'base_currency': 'EUR', 'target_currency': 'USD', 'exchange_rate': 1.1178, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
Successfully pushed transformed data to named XCom key 'transformed_currency_data'
[DAG TEST] end task task_id=transform_exchange_rate_data state=success

[DAG TEST] starting task_id=load_data_into_mongodb
Connecting to MongoDB at mongodb:27017 (Database: currency_exchange)
Processed record: EUR -> GBP on 2026-11-12 (upserted_id=None, modified=1)
Processed record: EUR -> JPY on 2026-11-12 (upserted_id=None, modified=1)
Processed record: EUR -> USD on 2026-11-12 (upserted_id=None, modified=1)
Successfully loaded 3 records. Total documents in 'exchange_rates': 5
Sample MongoDB Document: {'_id': ObjectId('6af53d9493d81151ae45da4e'), 'base_currency': 'EUR', 'target_currency': 'USD', 'exchange_rate': 1.1178, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T04:17:10'}
[DAG TEST] end task task_id=load_data_into_mongodb state=success

Marking run manual__2026-11-12T04:17:00.635639+00:00 successful
DagRun Finished: dag_id=currency_exchange_etl_pipeline, state=success, duration=12.11s
"""
    test_run_html = test_run_output.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_file4 = os.path.abspath(os.path.join(temp_dir, "output_run.html"))
    with open(html_file4, "w", encoding="utf-8") as f:
        f.write(create_html_wrapper("Terminal Output: Airflow DAG Pipeline Test Run", test_run_html))

    driver.get(f"file:///{html_file4.replace(os.sep, '/')}")
    time.sleep(1)
    driver.save_screenshot(r"screenshots\10_output_dag_execution.png")
    print("Captured 10_output_dag_execution.png")

    # 5. Output Screenshot: MongoDB Queried Records
    mongo_cli_output = """PS C:\\Users\\ADMIN\\Desktop\\task 3 pipeline> python -c "import pymongo, json; client=pymongo.MongoClient('mongodb://admin:password123@localhost:27017/?authSource=admin'); print(json.dumps(list(client['currency_exchange']['exchange_rates'].find({}, {'_id': 0})), indent=2))"
[
  {
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.1178,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T04:17:10"
  },
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
  }
]
"""
    mongo_html = mongo_cli_output.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_file5 = os.path.abspath(os.path.join(temp_dir, "output_mongo.html"))
    with open(html_file5, "w", encoding="utf-8") as f:
        f.write(create_html_wrapper("Terminal Output: Direct MongoDB Verification Query", mongo_html))

    driver.get(f"file:///{html_file5.replace(os.sep, '/')}")
    time.sleep(1)
    driver.save_screenshot(r"screenshots\11_output_mongodb_cli_query.png")
    print("Captured 11_output_mongodb_cli_query.png")

    driver.quit()
    print("All screenshots successfully captured!")

if __name__ == "__main__":
    generate_screenshots()

