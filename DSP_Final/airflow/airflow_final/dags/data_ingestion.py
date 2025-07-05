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
from sqlalchemy.exc import SQLAlchemyError
from great_expectations.core.batch import BatchRequest
import uuid
#---------------------------------------------------------------------------------------------------------------#


#----------------------------------------- Teams Notifications -------------------------------------------------#
TEAMS_WEBHOOK_URL = "https://epitafr.webhook.office.com/webhookb2/9b68f98c-e170-4cf5-b20a-d782e64d9f60@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/807dc258232949db93a77e437ba6c4e9/16aec797-5d29-44f3-9244-88bc228a3951/V2yAa0akyfZJ8Vcoq-jpe-ofrIIC1DCMUXzmR_dl6Reeg1"
#--------------------------------------- Project Root Directory ------------------------------------------------#
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)

from database.models import DataQualityStat
from database import SessionAirflow


# Initialize Airflow database
from database import init_airflow_db
init_airflow_db()
# --------------------------------------------- Logger Settings ------------------------------------------------#
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#---------------------------------------------------------------------------------------------------------------#
# Data Paths
#---------------------------------------------------------------------------------------------------------------#
RAW_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/raw_data"
GOOD_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/good_data"
BAD_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/bad_data"
DATA_DOCS_BASE_PATH = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/gx/uncommitted/data_docs/local_site"
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

#------------------------------------- Validate Data Function --------------------------------------------------#
def validate_data(file_path):
    """Validate the data in a file using Great Expectations."""
    validation_result = {
        'success': False,
        'file_path': file_path,
        'error_counts': {'validation_error': 1},
        'validation_time': datetime.now(timezone.utc).isoformat(),
        'valid_mask': None,
        'validation_id': None
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

        # Initialize GE validation
        context = get_ge_context()
        suite = load_expectation_suite(context)
        
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="spotify_data",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name=os.path.basename(file_path),
                data_connector_query={"index": -1}
            ),
            expectation_suite=suite,
            runtime_configuration={
                "result_format": {
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": ["__index__"]
                }
            }
        )
        
        raw_validation_result = validator.validate()
        
        # Create valid/invalid mask
        valid_mask = [True] * len(df)
        for result in raw_validation_result.results:
            if not result.success:
                if 'unexpected_index_list' in result.result:
                    for idx in result.result['unexpected_index_list']:
                        if isinstance(idx, int):
                            if idx < len(valid_mask):
                                valid_mask[idx] = False
                        else:
                            if idx[0] < len(valid_mask):
                                valid_mask[idx[0]] = False
        
        # Generate unique validation ID
        validation_id = str(uuid.uuid4())
        validation_result['validation_id'] = validation_id
        
        # Count errors by type
        error_counts = {
            'missing_values': 0,
            'invalid_types': 0,
            'out_of_range': 0,
            'invalid_format': 0,
            'other_errors': 0
        }
        
        for result in raw_validation_result.results:
            if not result.success:
                exp_type = result.expectation_config.expectation_type
                if "null" in exp_type:
                    error_counts['missing_values'] += 1
                elif "type" in exp_type:
                    error_counts['invalid_types'] += 1
                elif "between" in exp_type:
                    error_counts['out_of_range'] += 1
                elif "regex" in exp_type:
                    error_counts['invalid_format'] += 1
                else:
                    error_counts['other_errors'] += 1
        
        validation_result.update({
            'success': raw_validation_result.success,
            'valid_mask': valid_mask,
            'error_counts': error_counts,
            'ge_result': raw_validation_result.to_json_dict()  # Store serializable representation
        })
        return validation_result
        
    except Exception as e:
        logger.error(f"Critical validation error for {file_path}: {str(e)}")
        validation_result['error_counts'] = {'critical_error': 1}
        return validation_result

#------------------------------------- Save Statistics Function ------------------------------------------------#
def save_statistics(**kwargs):
    """Save data quality statistics to the Airflow database."""
    ti = kwargs['ti']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    
    session = None
    try:
        filename = os.path.basename(validation_result['file_path'])
        total_rows = len(validation_result['valid_mask']) if validation_result['valid_mask'] is not None else 0
        valid_rows = sum(validation_result['valid_mask']) if validation_result['valid_mask'] is not None else 0
        invalid_rows = total_rows - valid_rows
        
        session = SessionAirflow()
        
        # Save data quality statistics
        db_stat = DataQualityStat(
            filename=filename,
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            error_counts=json.dumps(validation_result['error_counts']),
            timestamp=validation_result['validation_time']
        )
        session.add(db_stat)
        session.commit()
        logger.info(f"Saved statistics to spotify_ml.data_quality_stats")
        
    except Exception as e:
        logger.error(f"Error saving statistics: {str(e)}")
        if session:
            session.rollback()
    finally:
        if session:
            session.close()

#-------------------------------------- Process Data Function -------------------------------------------------#
def process_data(**kwargs):
    """Process and move data based on validation results."""
    ti = kwargs['ti']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    file_path = validation_result['file_path']
    
    try:
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Handle validation failures
        if validation_result['valid_mask'] is None:
            # Critical error - move to bad_data
            dest_path = os.path.join(BAD_DATA_FOLDER, os.path.basename(file_path))
            os.makedirs(BAD_DATA_FOLDER, exist_ok=True)
            os.rename(file_path, dest_path)
            logger.info(f"Moved file with critical validation error to {dest_path}")
            return

        df = pd.read_csv(file_path)
        filename = os.path.basename(file_path)
        valid_mask = validation_result['valid_mask']
        
        if validation_result['success']:
            # All rows valid - move to good_data
            dest_path = os.path.join(GOOD_DATA_FOLDER, filename)
            os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
            os.rename(file_path, dest_path)
            logger.info(f"Moved valid file to {dest_path}")
            
        elif not any(valid_mask):
            # All rows invalid - move to bad_data
            dest_path = os.path.join(BAD_DATA_FOLDER, filename)
            os.makedirs(BAD_DATA_FOLDER, exist_ok=True)
            os.rename(file_path, dest_path)
            logger.info(f"Moved invalid file to {dest_path}")
            
        else:
            # Split into valid/invalid subsets
            valid_df = df[pd.Series(valid_mask)]
            invalid_df = df[~pd.Series(valid_mask)]
            
            # Save valid portion
            valid_path = os.path.join(GOOD_DATA_FOLDER, f"valid_{filename}")
            valid_df.to_csv(valid_path, index=False)
            
            # Save invalid portion
            invalid_path = os.path.join(BAD_DATA_FOLDER, f"invalid_{filename}")
            invalid_df.to_csv(invalid_path, index=False)
            
            # Remove original
            os.remove(file_path)
            logger.info(f"Split file into valid and invalid portions")
            
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        raise

#-------------------------------------- Save Alerts Function -------------------------------------------------#
def send_alerts(**kwargs):
    """Send alerts for data quality issues via Microsoft Teams with Data Docs."""
    ti = kwargs['ti']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    
    if not validation_result.get('success', False):
        try:
            # Generate HTML report
            context = get_ge_context()
            validation_id = validation_result.get('validation_id', str(uuid.uuid4()))
            report_filename = f"validation_report_{validation_id}.html"
            report_path = os.path.join(DATA_DOCS_BASE_PATH, report_filename)
            
            # Create simplified HTML report
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Data Quality Report - {os.path.basename(validation_result['file_path'])}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1 {{ color: #333; }}
                    .summary {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; }}
                    .error-counts {{ margin-top: 20px; }}
                    .error-counts table {{ border-collapse: collapse; width: 100%; }}
                    .error-counts th, .error-counts td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    .error-counts tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    .error-counts th {{ background-color: #4CAF50; color: white; }}
                </style>
            </head>
            <body>
                <h1>Data Quality Report</h1>
                <div class="summary">
                    <h2>Validation Summary</h2>
                    <p><strong>File:</strong> {os.path.basename(validation_result['file_path'])}</p>
                    <p><strong>Validation Time:</strong> {validation_result['validation_time']}</p>
                    <p><strong>Status:</strong> <span style="color: red;">FAILED</span></p>
                </div>
                
                <div class="error-counts">
                    <h2>Error Counts</h2>
                    <table>
                        <tr>
                            <th>Error Type</th>
                            <th>Count</th>
                        </tr>
                        <tr><td>Missing Values</td><td>{validation_result['error_counts']['missing_values']}</td></tr>
                        <tr><td>Invalid Types</td><td>{validation_result['error_counts']['invalid_types']}</td></tr>
                        <tr><td>Out of Range</td><td>{validation_result['error_counts']['out_of_range']}</td></tr>
                        <tr><td>Invalid Format</td><td>{validation_result['error_counts']['invalid_format']}</td></tr>
                        <tr><td>Other Errors</td><td>{validation_result['error_counts']['other_errors']}</td></tr>
                    </table>
                </div>
                
                <div class="details">
                    <h2>Validation Details</h2>
                    <pre>{json.dumps(validation_result['ge_result'], indent=2)}</pre>
                </div>
            </body>
            </html>
            """
            
            # Save HTML report
            os.makedirs(DATA_DOCS_BASE_PATH, exist_ok=True)
            with open(report_path, 'w') as f:
                f.write(html_content)
            
            # For local development, create a file URL
            report_url = f"file://{report_path}"
            
            # Determine criticality based on error rate
            total_rows = len(validation_result['valid_mask'])
            invalid_rows = total_rows - sum(validation_result['valid_mask'])
            error_ratio = invalid_rows / total_rows if total_rows > 0 else 0
            
            if error_ratio > 0.7:
                criticality = "HIGH"
                color = "FF0000"  # Red
            elif error_ratio > 0.3:
                criticality = "MEDIUM"
                color = "FFA500"  # Orange
            else:
                criticality = "LOW"
                color = "0076D7"  # Blue
            
            # Prepare Teams message
            alert_msg = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": color,
                "summary": "Data Quality Alert",
                "sections": [{
                    "activityTitle": f"DATA QUALITY ALERT - {criticality} CRITICALITY",
                    "facts": [
                        {"name": "File", "value": os.path.basename(validation_result['file_path'])},
                        {"name": "Total Rows", "value": total_rows},
                        {"name": "Invalid Rows", "value": invalid_rows},
                        {"name": "Error Ratio", "value": f"{error_ratio:.2%}"},
                        {"name": "Criticality", "value": criticality}
                    ],
                    "markdown": True
                }],
                "potentialAction": [{
                    "@type": "OpenUri",
                    "name": "View Full Report",
                    "targets": [{"os": "default", "uri": report_url}]
                }]
            }
            
            # Send to Teams
            response = requests.post(TEAMS_WEBHOOK_URL, json=alert_msg)
            if response.status_code != 200:
                logger.warning(f"Failed to send Teams alert: {response.status_code}, {response.text}")
            else:
                logger.info("Successfully sent Teams alert with Data Docs link")
                
        except Exception as e:
            logger.error(f"Exception while generating/sending alert: {str(e)}")
#---------------------------------------------------------------------------------------------------------------#
# DAGs Generation
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

    save_file = PythonOperator(
        task_id="save_file",
        python_callable=process_data,
        provide_context=True,
    )

    read_data_task >> validate_data_task >> [save_statistics_task, send_alerts_task, save_file]
#---------------------------------------------------------------------------------------------------------------#