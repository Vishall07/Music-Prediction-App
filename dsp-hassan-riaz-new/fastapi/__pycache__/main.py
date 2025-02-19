# api/main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import joblib
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, Boolean, TIMESTAMP

# Load the trained model
model = joblib.load("C:\\Users\\hassa\\Desktop\\DSP\\dsp-hassan-riaz-new\\models\\model.pkl")

# Database connection
DATABASE_URI = "postgresql+psycopg2://postgres:dragonhont@localhost:5432/spotify_db"
engine = create_engine(DATABASE_URI)
metadata = MetaData()

# Define the ingested_data table (for saving predictions)
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

# Create tables if they don't exist
metadata.create_all(engine)

# Define request models using Pydantic
class PredictionRequest(BaseModel):
    artist: str
    song: str
    duration_ms: int
    explicit: bool
    year: int
    danceability: float
    energy: float
    key: int
    loudness: float
    mode: int
    speechiness: float
    acousticness: float
    instrumentalness: float
    liveness: float
    valence: float
    tempo: float
    genre: str

class MultiPredictionRequest(BaseModel):
    data: List[PredictionRequest]

# Initialize FastAPI app
app = FastAPI()

@app.post("/prediction")
async def make_prediction(request: MultiPredictionRequest):
    """
    Endpoint for making predictions (single or multiple).
    """
    try:
        # Convert request data to DataFrame
        input_data = [item.dict() for item in request.data]
        df = pd.DataFrame(input_data)

        # Make predictions
        predictions = model.predict(df)

        # Save predictions and used features to the database
        df["popularity"] = predictions
        df["ingestion_timestamp"] = datetime.now()
        df.to_sql("ingested_data", engine, if_exists="append", index=False)

        # Return predictions
        return {"predictions": predictions.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/past-predictions")
async def get_past_predictions():
    """
    Endpoint for retrieving past predictions and used features.
    """
    try:
        # Query the database for past predictions
        query = "SELECT * FROM ingested_data;"
        df = pd.read_sql(query, engine)

        # Convert the DataFrame to a list of dictionaries
        past_predictions = df.to_dict(orient="records")

        # Return past predictions
        return {"past_predictions": past_predictions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)