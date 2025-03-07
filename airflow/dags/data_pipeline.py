from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import great_expectations as gx

# API Endpoints
FETCH_API_URL = "http://127.0.0.1:8000/get_data"
INJECTION_API_URL = "http://127.0.0.1:8000/inject_data"

# Default Airflow arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch data from FastAPI
def fetch_data():
    response = requests.get(FETCH_API_URL)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv("/tmp/raw_data.csv", index=False)
        print("Data fetched successfully!")
    else:
        raise Exception("Failed to fetch data.")

# Validate and clean data
def validate_data():
    df = pd.read_csv("/tmp/raw_data.csv")
    context = gx.get_context()

    gx_df = gx.from_pandas(df)
    gx_df.expect_column_values_to_not_be_null("column_name_1")
    gx_df.expect_column_values_to_not_be_null("column_name_2")

    df_cleaned = df.dropna()
    df_cleaned.to_csv("/tmp/cleaned_data.csv", index=False)
    print("Cleaned data saved.")

# Inject cleaned data into FastAPI
def inject_cleaned_data():
    df_cleaned = pd.read_csv("/tmp/cleaned_data.csv")
    response = requests.post(INJECTION_API_URL, json=df_cleaned.to_dict(orient="records"))
    
    if response.status_code == 200:
        print("Cleaned data sent to FastAPI!")
    else:
        raise Exception("Failed to inject data.")

# Define DAG
with DAG("great_expectations_pipeline", default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:

    task_fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch_data)
    task_validate_data = PythonOperator(task_id="validate_data", python_callable=validate_data)
    task_inject_data = PythonOperator(task_id="inject_cleaned_data", python_callable=inject_cleaned_data)

    task_fetch_data >> task_validate_data >> task_inject_data
