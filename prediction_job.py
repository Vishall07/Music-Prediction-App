
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Path to the good_data folder
GOOD_DATA_FOLDER = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/good_data/sample.csv"

# API endpoint URL
PREDICTION_API_URL = "http://localhost:8000/prediction"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "prediction_job",
    default_args=default_args,
    description="A DAG to make predictions on new data",
    schedule_interval="*/2 * * * *",  # Run every 2 minutes
    catchup=False,
)

def check_for_new_data(**kwargs):
    """
    Check for new files in the good_data folder.
    """
    # Get the list of files in the good_data folder
    files = os.listdir(GOOD_DATA_FOLDER)
    if not files:
        raise AirflowSkipException("No new data found. Skipping DAG run.")

    # Pass the list of files to the next task
    kwargs["ti"].xcom_push(key="new_files", value=files)

def make_predictions(**kwargs):
    """
    Make predictions using the API.
    """
    # Get the list of files from the previous task
    files = kwargs["ti"].xcom_pull(key="new_files", task_ids="check_for_new_data")

    for file in files:
        file_path = os.path.join(GOOD_DATA_FOLDER, file)
        df = pd.read_csv(file_path)

        # Prepare input data
        input_data = {"data": df.to_dict(orient="records")}

        # Make a request to the prediction API
        response = requests.post(PREDICTION_API_URL, json=input_data)

        if response.status_code == 200:
            predictions = response.json()["predictions"]
            df["popularity"] = predictions
            print(f"Predictions for {file}:")
            print(df)
        else:
            print(f"Failed to make predictions for {file}.")

# Define the tasks
check_for_new_data_task = PythonOperator(
    task_id="check_for_new_data",
    python_callable=check_for_new_data,
    provide_context=True,
    dag=dag,
)

make_predictions_task = PythonOperator(
    task_id="make_predictions",
    python_callable=make_predictions,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
check_for_new_data_task >> make_predictions_task