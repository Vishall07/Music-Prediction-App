import os
import shutil
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow.task")

# File paths
source_path = "/opt/airflow/dags/songs_1.csv"
destination_folder = "/opt/airflow/data/modified_files/"

# Function to check for modifications and move file
def check_and_move_file():
    try:
        if not os.path.exists(source_path):
            logger.info(f"File not found: {source_path}")
            return

        # Get file modification time
        last_modified_time = datetime.fromtimestamp(os.path.getmtime(source_path))
        current_time = datetime.now()
        time_difference = current_time - last_modified_time

        # Check if modified within the last day
        if time_difference <= timedelta(days=1):
            os.makedirs(destination_folder, exist_ok=True)  # Ensure target directory exists
            destination_path = os.path.join(destination_folder, os.path.basename(source_path))
            shutil.copy(source_path, destination_path)
            logger.info(f"File modified recently. Moved to: {destination_path}")
        else:
            logger.info("No recent modifications detected.")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise

# Define the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 20),
    "retries": 1,
}

with DAG(
    dag_id="Prediction_job",
    default_args=default_args,
    schedule_interval="@daily",  # Runs once a day
    catchup=False,
) as dag:

    monitor_task = PythonOperator(
        task_id="check_and_move_if_modified",
        python_callable=check_and_move_file,
    )

    monitor_task