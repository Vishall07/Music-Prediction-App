import os
import shutil
import pandas as pd
import psycopg2
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





from airflow import DAG
import pandas as pd
import psycopg2
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger("airflow.task")

# Function to extract data manually using psycopg2
def extract_to_excel():
    try:
        logger.info("Starting manual PostgreSQL connection test...")
        source_path = "/opt/airflow/dags/songs_1.csv"  # Source file path
        destination_path = "/opt/airflow/data/songs.csv"  # Destination folder

        logger.info("Starting CSV processing...")

        # Ensure file exists
        if not os.path.exists(source_path):
            logger.error(f"File not found: {source_path}")
            return

        # Read CSV file
        df = pd.read_csv(source_path)
        logger.info(f"Loaded {len(df)} rows from CSV.")

        # Move file after processing
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)  # Ensure target directory exists
        shutil.copy(source_path, destination_path)
        logger.info(f"CSV file moved to: {destination_path}")

      
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
    dag_id="Ingestion_job",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_manual",
        python_callable=extract_to_excel,
    )

    extract_task  