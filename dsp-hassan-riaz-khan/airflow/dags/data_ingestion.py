#---------------------------------------------------------------------------------------------------------------#
# Imports
#---------------------------------------------------------------------------------------------------------------#
import sys
import os
import random
import pandas as pd
import great_expectations as ge
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timezone

# ---------------------------- Project Root Directory (To Solve Path Issue) ------------------------------------#
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)
# --------------------------------------------------------------------------------------------------------------#

from database.models import DataQualityStat
from database.init_db import SessionLocal
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# Data Paths
#---------------------------------------------------------------------------------------------------------------#
RAW_DATA_FOLDER = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/raw_data"
GOOD_DATA_FOLDER = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/good_data"
BAD_DATA_FOLDER = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/bad_data"
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# Great Expectations
#---------------------------------------------------------------------------------------------------------------#

context = ge.data_context()

def read_data():
    # Randomly select a file from raw_data
    files = os.listdir(RAW_DATA_FOLDER)
    if not files:
        return None
    file_name = random.choice(files)
    return os.path.join(RAW_DATA_FOLDER, file_name)

def validate_data(file_path):
    # Load data into Great Expectations
    df = pd.read_csv(file_path)
    batch = context.get_batch({"path": file_path, "datasource": "spotify_data"}, "spotify_data_quality")

    # Validate data
    results = context.run_validation_operator("action_list_operator", [batch])

    # Return validation results
    return results["success"], results["results"]

def save_statistics(file_path, success, results):
    # Calculate statistics
    total_rows = len(pd.read_csv(file_path))
    valid_rows = total_rows if success else 0
    invalid_rows = total_rows - valid_rows

    # Save to database
    db = SessionLocal()
    db_stat = DataQualityStat(
        filename=os.path.basename(file_path),
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        error_counts={},
        timestamp=datetime.now(timezone.utc),
    )
    db.add(db_stat)
    db.commit()
    db.close()

def send_alerts(file_path, success, results):
    # Send alerts (e.g., via Teams or email)
    if not success:
        print(f"Data quality issues found in {file_path}. Check the report for details.")

def split_and_save_data(file_path, success, results):
    # Move file to good_data or bad_data folder
    if success:
        os.rename(file_path, os.path.join(GOOD_DATA_FOLDER, os.path.basename(file_path)))
    else:
        os.rename(file_path, os.path.join(BAD_DATA_FOLDER, os.path.basename(file_path)))
#---------------------------------------------------------------------------------------------------------------#


#---------------------------------------------------------------------------------------------------------------#
# Defining the DAGs
#---------------------------------------------------------------------------------------------------------------#
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "data_ingestion_dag",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
)

read_data_task = PythonOperator(
    task_id="read_data",
    python_callable=read_data,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    op_args=[read_data_task.output],
    dag=dag,
)

save_statistics_task = PythonOperator(
    task_id="save_statistics",
    python_callable=save_statistics,
    op_args=[read_data_task.output, validate_data_task.output[0], validate_data_task.output[1]],
    dag=dag,
)

send_alerts_task = PythonOperator(
    task_id="send_alerts",
    python_callable=send_alerts,
    op_args=[read_data_task.output, validate_data_task.output[0], validate_data_task.output[1]],
    dag=dag,
)

split_and_save_data_task = PythonOperator(
    task_id="split_and_save_data",
    python_callable=split_and_save_data,
    op_args=[read_data_task.output, validate_data_task.output[0], validate_data_task.output[1]],
    dag=dag,
)

# Task Dependencies
read_data_task >> validate_data_task
validate_data_task >> [save_statistics_task, send_alerts_task, split_and_save_data_task]
#---------------------------------------------------------------------------------------------------------------#