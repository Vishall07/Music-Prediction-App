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


# PostgreSQL connection details
DB_CONFIG = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "172.18.0.6",  # Change if needed
    "port": "5432"
}

# Create table query
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS songs (
    artist VARCHAR(255),
    song VARCHAR(255),
    duration_ms INT,
    explicit BOOLEAN,
    year INT,
    popularity INT,
    danceability FLOAT,
    energy FLOAT,
    feature1 INT,
    loudness FLOAT,
    feature2 INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    genre TEXT
);
"""

def process_csv():
    try:
        logger.info("Starting CSV processing...")

        # Ensure file exists
        if not os.path.exists(source_path):
            logger.error(f"File not found: {source_path}")
            return

        # Read CSV file
        df = pd.read_csv(source_path)
        logger.info(f"Loaded {len(df)} rows from CSV.")

        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create table if not exists
        cur.execute(CREATE_TABLE_QUERY)
        conn.commit()
        logger.info("Table 'songs' ensured to exist.")

        # Insert data into PostgreSQL
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO songs (
                    artist, song, duration_ms, explicit, year, popularity,
                    danceability, energy, feature1, loudness, feature2,
                    speechiness, acousticness, instrumentalness, liveness,
                    valence, tempo, genre
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["artist"], row["song"], row["duration_ms"], row["explicit"], row["year"],
                    row["popularity"], row["danceability"], row["energy"], row["feature1"], row["loudness"],
                    row["feature2"], row["speechiness"], row["acousticness"], row["instrumentalness"],
                    row["liveness"], row["valence"], row["tempo"], row["genre"]
                )
            )

        conn.commit()
        logger.info("Data inserted into PostgreSQL successfully.")

        # Move file after processing
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)  # Ensure target directory exists
        shutil.move(source_path, destination_path)
        logger.info(f"CSV file moved to: {destination_path}")

        # Close database connection
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)




from airflow import DAG
from airflow.operators.python import PythonOperator
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
        destination_path = "/opt/airflow/logs/songs.csv"  # Destination folder

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
    dag_id="a_extract_postgres_to_excel_manual",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_manual",
        python_callable=extract_to_excel,
    )

    extract_task  