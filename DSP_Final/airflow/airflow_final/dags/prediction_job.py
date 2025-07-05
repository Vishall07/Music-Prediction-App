#---------------------------------------------------------------------------------------------------------------#
#                           PREDICTION DAG WITH DUPLICATE HANDLING (UPDATED VERSION)
#---------------------------------------------------------------------------------------------------------------#
import os
import json
import logging
import datetime
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from requests.exceptions import RequestException
from psycopg2 import sql
from collections import defaultdict

GOOD_DATA_FOLDER = "/mnt/c/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/good_data"
WINDOWS_HOST_IP = "172.24.160.1"
PREDICTION_API_URL = f"http://{WINDOWS_HOST_IP}:8000/predict"
PROCESSED_FILES_CACHE = os.path.join(os.path.dirname(__file__), "processed_files.json")

PG_HOST = "172.24.160.89"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "postgres"
PG_PASSWORD = "dragonhont"
PG_TABLE = "scheduled_predictions"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# DATABASE SETUP FUNCTIONS
# --------------------------------------------------
def ensure_table_exists():
    """Ensure the table exists with proper constraints"""
    conn = None
    try:
        conn = create_pg_connection()
        cursor = conn.cursor()
        
        cursor.execute(sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """), [PG_TABLE])
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            cursor.execute(sql.SQL("""
                CREATE TABLE {} (
                    prediction_id SERIAL PRIMARY KEY,
                    song TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    energy FLOAT NOT NULL,
                    loudness FLOAT NOT NULL,
                    genre TEXT NOT NULL,
                    predicted_popularity FLOAT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_prediction UNIQUE (song, duration_ms, year)
                );
            """).format(sql.Identifier(PG_TABLE)))
            logger.info("Created table with constraints")
        else:
            cursor.execute(sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM pg_constraint
                    WHERE conname = 'unique_prediction'
                    AND conrelid = %s::regclass
                );
            """), [PG_TABLE])
            
            constraint_exists = cursor.fetchone()[0]
            
            if not constraint_exists:
                logger.warning("unique_prediction constraint missing, adding...")
                cursor.execute(sql.SQL("""
                    ALTER TABLE {} 
                    ADD CONSTRAINT unique_prediction 
                    UNIQUE (song, duration_ms, year);
                """).format(sql.Identifier(PG_TABLE)))
                
        conn.commit()
        logger.info("Table verification complete")
        
    except Exception as e:
        logger.error(f"Table verification failed: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

# --------------------------------------------------
# FILE TRACKING SYSTEM
# --------------------------------------------------
def init_file_tracking():
    if not os.path.exists(PROCESSED_FILES_CACHE):
        with open(PROCESSED_FILES_CACHE, 'w') as f:
            json.dump({"processed_files": []}, f)

def get_file_fingerprint(file_path):
    stat = os.stat(file_path)
    return f"{stat.st_size}_{stat.st_mtime_ns}"

def check_for_new_data(**kwargs):
    try:
        init_file_tracking()
        with open(PROCESSED_FILES_CACHE, 'r') as f:
            tracking_data = json.load(f)
        
        processed_entries = tracking_data.get("processed_files", [])
        processed_map = {e['filename']: e['fingerprint'] for e in processed_entries}
        
        current_files = []
        for filename in os.listdir(GOOD_DATA_FOLDER):
            if filename.lower().endswith('.csv'):
                file_path = os.path.join(GOOD_DATA_FOLDER, filename)
                if os.path.isfile(file_path):
                    current_files.append({
                        "filename": filename,
                        "fingerprint": get_file_fingerprint(file_path)
                    })
        
        new_files = [
            f['filename'] for f in current_files
            if f['filename'] not in processed_map or 
            f['fingerprint'] != processed_map.get(f['filename'])
        ]
        
        if not new_files:
            logger.info("No new files found")
            raise AirflowSkipException("No new files to process")
        
        logger.info(f"Found {len(new_files)} new/changed files")
        return new_files

    except Exception as e:
        logger.error(f"File detection error: {str(e)}", exc_info=True)
        raise

# --------------------------------------------------
# DATABASE FUNCTIONS (WITH DUPLICATE HANDLING)
# --------------------------------------------------
def create_pg_connection():
    """Create PostgreSQL connection with optimized settings"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

def save_predictions_batch(batch):
    """Save multiple predictions with duplicate handling"""
    conn = None
    try:
        conn = create_pg_connection()
        cursor = conn.cursor()
        
        unique_batch = []
        seen = set()
        
        for item in batch:
            key = (item['song'], item['duration_ms'], item['year'])
            if key not in seen:
                seen.add(key)
                unique_batch.append(item)
        
        if len(unique_batch) < len(batch):
            logger.warning(f"Removed {len(batch)-len(unique_batch)} duplicates from batch")
        
        if not unique_batch:
            return 0
        
        chunk_size = 50
        total_inserted = 0
        
        for i in range(0, len(unique_batch), chunk_size):
            chunk = unique_batch[i:i + chunk_size]
            
            query = sql.SQL("""
                INSERT INTO {} 
                (song, duration_ms, year, energy, loudness, genre, predicted_popularity)
                VALUES %s
                ON CONFLICT ON CONSTRAINT unique_prediction DO UPDATE 
                SET energy = EXCLUDED.energy,
                    loudness = EXCLUDED.loudness,
                    genre = EXCLUDED.genre,
                    predicted_popularity = EXCLUDED.predicted_popularity,
                    processed_at = CURRENT_TIMESTAMP
            """).format(sql.Identifier(PG_TABLE))
            
            values = [(p['song'], p['duration_ms'], p['year'], 
                    p['energy'], p['loudness'], p['genre'],
                    p['predicted_popularity']) for p in chunk]
            
            try:
                extras.execute_values(cursor, query, values)
                conn.commit()
                total_inserted += len(chunk)
                logger.debug(f"Successfully inserted {len(chunk)} records")
            except psycopg2.Error as e:
                logger.error(f"Chunk insert failed (code {e.pgcode}): {e.pgerror}")
                conn.rollback()
                # Fallback to individual inserts for problematic chunks
                for record in chunk:
                    try:
                        cursor.execute(sql.SQL("""
                            INSERT INTO {} 
                            (song, duration_ms, year, energy, loudness, genre, predicted_popularity)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT unique_prediction DO UPDATE 
                            SET energy = EXCLUDED.energy,
                                loudness = EXCLUDED.loudness,
                                genre = EXCLUDED.genre,
                                predicted_popularity = EXCLUDED.predicted_popularity,
                                processed_at = CURRENT_TIMESTAMP
                        """).format(sql.Identifier(PG_TABLE)), (
                            record['song'], record['duration_ms'], record['year'],
                            record['energy'], record['loudness'], record['genre'],
                            record['predicted_popularity']
                        ))
                        conn.commit()
                        total_inserted += 1
                    except Exception as e:
                        logger.error(f"Failed to insert individual record: {str(e)}")
                        conn.rollback()
        
        return total_inserted
        
    except Exception as e:
        logger.error(f"Batch insert failed: {str(e)}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            conn.close()

# --------------------------------------------------
# PREDICTION PROCESSING (WITH DUPLICATE HANDLING)
# --------------------------------------------------
def make_predictions(**kwargs):
    # Ensure table exists before processing
    ensure_table_exists()
    
    ti = kwargs['ti']
    new_files = ti.xcom_pull(task_ids='check_for_new_data')
    
    if not new_files:
        logger.info("No files to process")
        return

    with open(PROCESSED_FILES_CACHE, 'r') as f:
        tracking_data = json.load(f)
    
    processed_entries = tracking_data.get("processed_files", [])
    
    for filename in new_files:
        file_path = os.path.join(GOOD_DATA_FOLDER, filename)
        current_fingerprint = get_file_fingerprint(file_path)
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {filename}")
                continue
                
            df = pd.read_csv(file_path)
            required_cols = ['song', 'duration_ms', 'year', 'energy', 'loudness', 'genre']
            
            if not all(col in df.columns for col in required_cols):
                logger.error(f"Missing columns in {filename}")
                continue
                
            success_count = 0
            batch = []
            batch_size = 100  # Optimal batch size for performance
            
            for _, row in df.iterrows():
                try:
                    payload = {
                        "song": str(row['song']),
                        "duration_ms": int(row['duration_ms']),
                        "year": int(row['year']),
                        "energy": float(row['energy']),
                        "loudness": float(row['loudness']),
                        "genre": str(row['genre'])
                    }
                    
                    # Make API request with timeout and retry
                    try:
                        response = requests.post(
                            PREDICTION_API_URL,
                            json=payload,
                            timeout=10
                        )
                        response.raise_for_status()
                    except RequestException as e:
                        logger.error(f"API request failed for {row['song']}: {str(e)}")
                        continue
                    
                    # Process API response
                    try:
                        result = response.json()
                        logger.debug(f"API response for {row['song']}: {result}")
                        
                        # Handle different response formats
                        if isinstance(result, list) and len(result) > 0:
                            prediction_value = result[0].get('prediction')
                        elif isinstance(result, dict):
                            prediction_value = result.get('prediction')
                        else:
                            logger.error(f"Unexpected API response format: {result}")
                            continue
                            
                        if prediction_value is None:
                            logger.error(f"No prediction value in response for {row['song']}")
                            continue
                            
                        batch.append({
                            **payload,
                            "predicted_popularity": prediction_value
                        })
                        
                        if len(batch) >= batch_size:
                            inserted = save_predictions_batch(batch)
                            success_count += inserted
                            batch = []
                            
                    except Exception as e:
                        logger.error(f"Failed to process API response for {row['song']}: {str(e)}")
                        continue
                    
                except Exception as e:
                    logger.error(f"Record processing failed: {str(e)}")
                    continue
            
            # Process remaining records in batch
            if batch:
                inserted = save_predictions_batch(batch)
                success_count += inserted
            
            if success_count > 0:
                processed_entries.append({
                    "filename": filename,
                    "fingerprint": current_fingerprint,
                    "processed_at": datetime.datetime.now().isoformat(),
                    "successful_records": success_count
                })
                logger.info(f"Successfully processed {success_count}/{len(df)} records from {filename}")
            else:
                logger.warning(f"No successful predictions for {filename}")

        except Exception as e:
            logger.error(f"File processing failed: {str(e)}")
            continue

    # Update tracking file
    tracking_data["processed_files"] = processed_entries
    with open(PROCESSED_FILES_CACHE, 'w') as f:
        json.dump(tracking_data, f, indent=2)

# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": 300,
}

dag = DAG(
    "prediction_job",
    default_args=default_args,
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["predictions"]
)

# Task definitions
check_task = PythonOperator(
    task_id="check_for_new_data",
    python_callable=check_for_new_data,
    provide_context=True,
    dag=dag,
)

predict_task = PythonOperator(
    task_id="make_predictions",
    python_callable=make_predictions,
    provide_context=True,
    dag=dag,
)

# Task dependencies
check_task >> predict_task