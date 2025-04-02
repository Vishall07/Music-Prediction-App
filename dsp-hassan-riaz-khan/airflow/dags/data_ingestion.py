#---------------------------------------------------------------------------------------------------------------#
#                                            DATA INGESTION DAG
#---------------------------------------------------------------------------------------------------------------#





#---------------------------------------------------------------------------------------------------------------#
# Imports
#---------------------------------------------------------------------------------------------------------------#
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
import great_expectations as ge
from datetime import datetime, timezone
import logging
import random
import sys
import json
import requests


TEAMS_WEBHOOK_URL = "https://your-teams-webhook-url"
from sqlalchemy.exc import SQLAlchemyError
from great_expectations.core.batch import BatchRequest


#------------------------------------------- Project Root Directory -------------------------------------------#
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)

from database.models import DataQualityStat
from database.init_db import SessionLocal

# --------------------------------------------- Logger Settings ------------------------------------------------#
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#---------------------------------------------------------------------------------------------------------------#
# Data Paths
#---------------------------------------------------------------------------------------------------------------#
RAW_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/raw_data"
GOOD_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/good_data"
BAD_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/bad_data"

#---------------------------------------------------------------------------------------------------------------#
# Great Expectations Setup
#---------------------------------------------------------------------------------------------------------------#
def get_ge_context():
    """Initialize and return Great Expectations context"""
    try:
        return ge.data_context.DataContext('/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/gx')
    except Exception as e:
        logger.error(f"Failed to initialize GE context: {str(e)}")
        raise

def load_expectation_suite(context=None):
    """Load the expectation suite"""
    try:
        if context is None:
            context = get_ge_context()
        return context.get_expectation_suite("spotify_data_quality")
    except Exception as e:
        logger.error(f"Failed to load expectation suite: {str(e)}")
        raise

#---------------------------------------------------------------------------------------------------------------#
# Core Functions
#---------------------------------------------------------------------------------------------------------------#
def read_data():    
    """Randomly select a file from the raw_data folder."""
    try:
        files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith('.csv')]
        if not files:
            logger.info("No CSV files found in the raw_data folder.")
            return None
            
        file_name = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, file_name)
        logger.info(f"Selected file for processing: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error in read_data: {str(e)}")
        raise
#---------------------------------------------------------------------------------------------------------------#

def validate_data(file_path):
    """Validate the data in a file using Great Expectations."""
    validation_result = {
        'success': False,
        'results': [],
        'file_path': file_path,
        'error_counts': {'validation_error': 1},
        'validation_time': datetime.now(timezone.utc).isoformat()
    }

    try:
        if not file_path or not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return validation_result

        try:
            df = pd.read_csv(file_path)
            if df.empty:
                logger.warning(f"Empty file: {file_path}")
                validation_result['error_counts'] = {'empty_file': 1}
                return validation_result
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {str(e)}")
            validation_result['error_counts'] = {'file_read_error': 1}
            return validation_result

#-------------------------------------- Initialising GE Validation --------------------------------------------#
        context = get_ge_context()
        suite = load_expectation_suite(context)
        
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="spotify_data",
                data_connector_name= "default_inferred_data_connector_name",
                data_asset_name=os.path.basename(file_path),
                data_connector_query= {"index": -1}
            ),
            expectation_suite=suite,
            runtime_configuration={"result_format": "COMPLETE"}
        )
        
        validation_result = validator.validate()
        
        error_counts = {
            'missing_values': 0,
            'invalid_types': 0,
            'out_of_range': 0,
            'invalid_format': 0,
            'other_errors': 0
        }
        
        if not validation_result["success"]:
            for result in validation_result["results"]:
                if not result["success"]:
                    if "null" in result["expectation_config"]["expectation_type"]:
                        error_counts['missing_values'] += 1
                    elif "type" in result["expectation_config"]["expectation_type"]:
                        error_counts['invalid_types'] += 1
                    elif "between" in result["expectation_config"]["expectation_type"]:
                        error_counts['out_of_range'] += 1
                    elif "regex" in result["expectation_config"]["expectation_type"]:
                        error_counts['invalid_format'] += 1
                    else:
                        error_counts['other_errors'] += 1

            logger.warning(f"Validation failed for {file_path} with:")
            for err_type, count in error_counts.items():
                if count > 0:
                    logger.warning(f" - {err_type}: {count} violations")

        validation_result = validation_result.to_json_dict()
        logger.info(f"Validation completed for {file_path}. Success: {validation_result}")

        return {
            'success': validation_result["success"],
            'results': validation_result["results"],
            'file_path': file_path,
            'error_counts': error_counts,
            'validation_time': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Critical validation error for {file_path}: {str(e)}")
        validation_result['error_counts'] = {'critical_error': 1}
        return validation_result
#---------------------------------------------------------------------------------------------------------------#

def save_statistics(**kwargs):
    """Save data quality statistics to the database."""
    ti = kwargs['ti']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    
    db = None
    try:
        df = pd.read_csv(validation_result['file_path'])
        total_rows = len(df)
        valid_rows = total_rows if validation_result['success'] else 0
        invalid_rows = total_rows - valid_rows

        db = SessionLocal()
        db_stat = DataQualityStat(
            filename=os.path.basename(validation_result['file_path']),
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            error_counts=json.dumps(validation_result['error_counts']),
            timestamp=validation_result['validation_time']
        )
        db.add(db_stat)
        db.commit()
        logger.info(f"Saved statistics for {validation_result['file_path']} to database")
        
    except Exception as e:
        logger.error(f"Error saving statistics: {str(e)}")
        if db:
            db.rollback()
    finally:
        if db:
            db.close()
#---------------------------------------------------------------------------------------------------------------#

def send_alerts(**kwargs):
    """Send alerts for data quality issues."""
    ti = kwargs['ti']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    
    if not validation_result['success'] and validation_result['error_counts']:
        alert_msg = f"Data quality alert for {os.path.basename(validation_result['file_path'])}:\n"
        alert_msg += "\n".join([f"- {k}: {v} violations" for k,v in validation_result['error_counts'].items()])
        logger.warning(alert_msg)

    try:
        payload = {
            "text": alert_msg
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(TEAMS_WEBHOOK_URL, json=payload, headers=headers)

        if response.status_code == 200:
            logger.info("✅ Alert successfully sent to Teams")
        else:
            logger.error(f"❌ Failed to send alert: {response.text}")

    except Exception as e:
        logger.error(f"❌ Error sending alert: {str(e)}")
#---------------------------------------------------------------------------------------------------------------#

def split_and_save_data(**kwargs):
    """Move the file to the appropriate folder based on validation results."""
    ti = kwargs['ti']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    file_path = validation_result['file_path']
    
    try:
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        dest_folder = GOOD_DATA_FOLDER if validation_result['success'] else BAD_DATA_FOLDER
        dest_path = os.path.join(dest_folder, os.path.basename(file_path))
        
        os.makedirs(dest_folder, exist_ok=True)
        os.rename(file_path, dest_path)
        logger.info(f"Moved {file_path} to {dest_folder}")
        
    except Exception as e:
        logger.error(f"Error moving file {file_path}: {str(e)}")
        raise

#---------------------------------------------------------------------------------------------------------------#
# DAG Definition
#---------------------------------------------------------------------------------------------------------------#
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": 30,
}

dag = DAG(
    "data_ingestion",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data_quality"],
)

with dag:
    read_data_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_args=[read_data_task.output],
    )

    save_statistics_task = PythonOperator(
        task_id="save_statistics",
        python_callable=save_statistics,
        provide_context=True,
    )

    send_alerts_task = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
        provide_context=True,
    )

    split_and_save_data_task = PythonOperator(
        task_id="split_and_save_data",
        python_callable=split_and_save_data,
        provide_context=True,
    )

    read_data_task >> validate_data_task >> [save_statistics_task, send_alerts_task, split_and_save_data_task]

#---------------------------------------------------------------------------------------------------------------#
