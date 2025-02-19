
import great_expectations as ge
from great_expectations.data_context import DataContext  # Correct import for newer versions
import pandas as pd
from datetime import datetime
import logging
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, Boolean, TIMESTAMP

#------------------------- Logging --------------------------#
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data/validated/validation_logs.log"),
        logging.StreamHandler()
    ]
)
#------------------------------------------------------------#


#--------------------- Database connection ---------------------#
DATABASE_URI = "postgresql+psycopg2://postgres:dragonhont@localhost:5432/spotify_ml"
engine = create_engine(DATABASE_URI)
metadata = MetaData()
#---------------------------------------------------------------#


# Define tables
ingested_data = Table(
    "ingested_data", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("artist", String),
    Column("song", String),
    Column("duration_ms", Integer),
    Column("explicit", Boolean),
    Column("year", Integer),
    Column("popularity", Integer),
    Column("danceability", Float),
    Column("energy", Float),
    Column("key", Integer),
    Column("loudness", Float),
    Column("mode", Integer),
    Column("speechiness", Float),
    Column("acousticness", Float),
    Column("instrumentalness", Float),
    Column("liveness", Float),
    Column("valence", Float),
    Column("tempo", Float),
    Column("genre", String),
    Column("ingestion_timestamp", TIMESTAMP)
)

validation_results = Table(
    "validation_results", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("expectation_type", String),
    Column("column_name", String),
    Column("error_message", String),
    Column("validation_timestamp", TIMESTAMP)
)

# Create tables if they don't exist
metadata.create_all(engine)

def validate_data():
    """
    Validate the ingested data using Great Expectations.
    """
    # Load the ingested data
    df = pd.read_csv("data/processed/new_data.csv")

    # Convert the DataFrame to a Great Expectations DataFrame
    ge_df = DataContext(df)  # Correct usage

    # Define validation rules
    ge_df.expect_column_values_to_not_be_null("artist")
    ge_df.expect_column_values_to_not_be_null("song")
    ge_df.expect_column_values_to_be_of_type("year", "int64")
    ge_df.expect_column_values_to_be_between("popularity", min_value=0, max_value=100)

    # Validate the data
    validation_result = ge_df.validate()

    # Check if validation failed
    if not validation_result["success"]:
        logging.error("Data quality issues detected!")
        for result in validation_result["results"]:
            if not result["success"]:
                logging.error(f"Failed expectation: {result['expectation_config']['expectation_type']}")
                logging.error(f"Details: {result['result']}")
                save_validation_result(
                    result['expectation_config']['expectation_type'],
                    result['expectation_config']['kwargs'].get('column'),
                    str(result['result'])
                )
    else:
        logging.info("Data validation successful!")

def save_validation_result(expectation_type, column_name, error_message):
    """
    Save a validation result to the database.
    """
    with engine.connect() as connection:
        insert_query = validation_results.insert().values(
            expectation_type=expectation_type,
            column_name=column_name,
            error_message=error_message,
            validation_timestamp=datetime.now()
        )
        connection.execute(insert_query)
    logging.info("Saved validation result to the database.")

# Run validation
validate_data()