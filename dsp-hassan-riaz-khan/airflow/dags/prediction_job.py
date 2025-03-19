#---------------------------------------------------------------------------------------------------------------#
# Imports
#---------------------------------------------------------------------------------------------------------------#
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import logging
import os
import pandas as pd
import requests
from datetime import datetime
import sys
from sqlalchemy.exc import SQLAlchemyError
from airflow.utils.dates import days_ago

#------------------------------------------ Logger Configuration -----------------------------------------------#
from datetime import timezone
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#---------------------------- Project Root Directory (To Solve Path Issue) -------------------------------------#
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)
    
#---------------------------------------------------------------------------------------------------------------#

from database.models import Prediction, ProcessedFile
from database.init_db import SessionLocal
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# Data Paths
#---------------------------------------------------------------------------------------------------------------#
GOOD_DATA_FOLDER = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/good_data"
PROCESSED_FILES_TABLE = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/processed_files"
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# API URL
#---------------------------------------------------------------------------------------------------------------#
PREDICTION_API_URL = "http://localhost:8000/predict"
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# Folders and Files Access Points
#---------------------------------------------------------------------------------------------------------------#
def check_for_new_data():
    # Get list of files in good_data folder
    files = os.listdir(GOOD_DATA_FOLDER)
    if not files:
        return None

    # Get list of already processed files
    db = SessionLocal()
    processed_files = db.query(PROCESSED_FILES_TABLE).all()
    processed_files = [row.filename for row in processed_files]
    db.close()

    # Filter out processed files
    return [f for f in files if f not in processed_files]
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# API Access
#---------------------------------------------------------------------------------------------------------------#
def make_predictions(new_files):
    """
    Make predictions for new files and save results to the database.
    """
    if not new_files:
        logger.info("No new files to process. Skipping predictions.")
        return

    for file in new_files:
        file_path = os.path.join(GOOD_DATA_FOLDER, file)
        logger.info(f"Processing file: {file}")

        try:
            # Read the file
            df = pd.read_csv(file_path)
            logger.info(f"Successfully read {len(df)} rows from {file}.")

            # Prepare input data for the API
            input_data = {"data": df.to_dict(orient="records")}

            # Make predictions using the API
            response = requests.post(PREDICTION_API_URL, json=input_data)
            if response.status_code != 200:
                logger.error(f"Failed to make predictions for {file}. Status code: {response.status_code}")
                continue

            # Save predictions to the database
            predictions = response.json()["predictions"]
            db = SessionLocal()
            for i, row in df.iterrows():
                db_prediction = Prediction(
                    features=row.to_dict(),
                    prediction_result={"popularity": predictions[i]},
                    source="SCHEDULED",
                    timestamp=datetime.now(timezone.utc),
                )
                db.add(db_prediction)
            db.commit()
            logger.info(f"Successfully saved predictions for {file} to the database.")

            # Mark the file as processed
            db_processed_file = ProcessedFile(
                filename=file, processed_at=datetime.now(timezone.utc)
            )
            db.add(db_processed_file)
            db.commit()
            logger.info(f"Marked {file} as processed.")

        except pd.errors.EmptyDataError:
            logger.error(f"File {file} is empty. Skipping.")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {file}: {e}")
        except SQLAlchemyError as e:
            logger.error(f"Database error while processing {file}: {e}")
            db.rollback()
        except Exception as e:
            logger.error(f"Unexpected error while processing {file}: {e}")
        finally:
            if 'db' in locals():
                db.close()
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
    "prediction_job",
    default_args=default_args,
    schedule_interval="*/2 * * * *",
    catchup=False,
)


check_for_new_data_task = PythonOperator(
    task_id="check_for_new_data",
    python_callable=check_for_new_data,
    dag=dag,
)

make_predictions_task = PythonOperator(
    task_id="make_predictions",
    python_callable=make_predictions,
    op_args=[check_for_new_data_task.output],
    dag=dag,
)

check_for_new_data_task >> make_predictions_task
#---------------------------------------------------------------------------------------------------------------#