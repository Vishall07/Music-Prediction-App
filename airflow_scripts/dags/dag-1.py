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

        # ✅ Manually enter PostgreSQL connection details
        conn = psycopg2.connect(
            dbname="airflow",   # Replace with your database name
            user="airflow",     # Replace with your username
            password="airflow",  # Replace with your password
            host="172.18.0.6",    # Try '127.0.0.1' if 'localhost' fails
            port="5432"          # Default PostgreSQL port
        )
        logger.info("Successfully connected to PostgreSQL!")

        # Create a cursor and execute the query
        cur = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            position VARCHAR(100),
            salary DECIMAL(10,2),
            hire_date DATE
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        logger.info("Table 'employees' created successfully (if not exists).")

        # Step 2: Insert dummy data
        insert_data_query = """
        INSERT INTO employees (name, position, salary, hire_date) VALUES
        ('Alice Johnson', 'Software Engineer', 75000, '2023-05-10'),
        ('Bob Smith', 'Project Manager', 90000, '2022-11-15'),
        ('Charlie Brown', 'Data Analyst', 65000, '2021-06-20')
        RETURNING *;
        """
        cur.execute(insert_data_query)
        inserted_rows = cur.fetchall()
        conn.commit()
        logger.info(f"Inserted data: {inserted_rows}")

        # Step 3: Fetch and log all employee data
        sql_query = "SELECT * FROM employees"
        logger.info(f"Executing query: {sql_query}")
        cur.execute(sql_query)
        all_rows = cur.fetchall()
        logger.info(f"Fetched data: {all_rows}")
        # Fetch data into pandas DataFrame
        columns = [desc[0] for desc in cur.description]  # Get column names
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        logger.info(f"Query executed successfully. Rows fetched: {len(df)}")

        # Save to Excel
        file_path = "/tmp/postgres_data_manual.xlsx"
        df.to_excel(file_path, index=False, engine='openpyxl')
        logger.info(f"Excel file saved successfully at: {file_path}")

        # Close connection
        cur.close()
        conn.close()

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
    dag_id="G_extract_postgres_to_excel_manual",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_manual",
        python_callable=extract_to_excel,
    )

    extract_task  
