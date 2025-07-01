from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2

def fetch_api_songs():
    url = "http://fastapi-backend:8000/songs"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    with open("/opt/airflow/logs/api_songs.txt", "w") as f:
        f.write(str(data))

def fetch_db_songs():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="mydb",
        user="myuser",
        password="mypass"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM songs;")
    rows = cursor.fetchall()
    with open("/opt/airflow/logs/db_songs.txt", "w") as f:
        for row in rows:
            f.write(str(row) + "\n")
    cursor.close()
    conn.close()

with DAG(
    dag_id="fetch_songs_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="* * * * *",  # every 1 minute
    catchup=False,
) as dag:

    fetch_api = PythonOperator(
        task_id="fetch_api_songs",
        python_callable=fetch_api_songs,
    )

    fetch_db = PythonOperator(
        task_id="fetch_db_songs",
        python_callable=fetch_db_songs,
    )

    fetch_api >> fetch_db
