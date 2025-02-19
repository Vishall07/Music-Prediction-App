# ingestion/ingest_data.py

import time
import pandas as pd
from datetime import datetime

def ingest_new_data():
    """
    Simulate ingesting new data every 1 minute.
    """

    df = pd.read_csv("datasets/raw/spotify_dataset.csv")

    # Sample 5 rows from the dataset
    new_data = df.sample(n=5)  # Ingest 5 rows every minute

    # Add a timestamp for when the data was ingested
    new_data["ingestion_timestamp"] = datetime.now()

    # Save the new data to a file (for demonstration purposes)
    new_data.to_csv("datasets/processed/new_data.csv", index=False)

    print(f"Ingested {len(new_data)} rows at {datetime.now()}")

# Simulate continuous ingestion
while True:
    ingest_new_data()
    time.sleep(60)  # Wait for 1 minute