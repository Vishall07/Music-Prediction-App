from fastapi import FastAPI, Depends, Query, UploadFile, File
from sqlalchemy.orm import Session
import pandas as pd
import io
import models
from database import SessionLocal, engine
import datetime

models.Base.metadata.create_all(bind=engine)

from models import Base
from database import engine

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

from models import DataEntry
from sqlalchemy.exc import SQLAlchemyError

@app.post("/predict/")
async def predict(
    file: UploadFile = File(...),
    danceability_min: float = Query(..., description="Minimum danceability"),
    danceability_max: float = Query(..., description="Maximum danceability"),
    tempo_min: float = Query(..., description="Minimum tempo"),
    tempo_max: float = Query(..., description="Maximum tempo"),
    genre: str = Query("All", description="Genre to filter"),
    db: Session = Depends(get_db),
):
    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    except Exception as e:
        return {"error": f"Failed to process CSV file: {str(e)}"}

    if not {"danceability", "tempo", "genre"}.issubset(df.columns):
        return {"error": "CSV must contain 'danceability', 'tempo', and 'genre' columns"}

    df["danceability"] = pd.to_numeric(df["danceability"], errors="coerce")
    df["tempo"] = pd.to_numeric(df["tempo"], errors="coerce")
    df["genre"] = df["genre"].fillna("Unknown")

    filtered_df = df[
        (df["danceability"] >= danceability_min) & 
        (df["danceability"] <= danceability_max) & 
        (df["tempo"] >= tempo_min) & 
        (df["tempo"] <= tempo_max)
    ]

    if genre.lower() != "all":
        filtered_df = filtered_df[filtered_df["genre"].str.contains(genre, case=False, na=False)]

    if filtered_df.empty:
        return {"error": "No data found after applying the filters."}

    results = filtered_df.to_dict(orient="records")

    try:
        for row in results:
            new_entry = DataEntry(
                feature1=row["danceability"],
                feature2=row["tempo"],
                prediction=row["genre"]
            )
            db.add(new_entry)
        
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        return {"error": "Database error", "details": str(e)}

    return {"message": "Filtered predictions", "results": results}

@app.get("/past-predictions/")
def get_past_predictions(db: Session = Depends(get_db)):
    predictions = db.query(DataEntry).all()
    return predictions
