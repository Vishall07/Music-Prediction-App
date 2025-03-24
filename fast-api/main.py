from fastapi import FastAPI, Depends, Query, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
import pandas as pd
import io
import models
from database import SessionLocal, engine
import datetime
from models import DataEntry, UploadedCSV
from sqlalchemy.exc import SQLAlchemyError

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Database Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Utility Function to Process CSV
def process_csv(file: UploadFile):
    try:
        contents = file.file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to process CSV file: {str(e)}")

    required_columns = {"danceability", "tempo", "genre"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=400, detail="CSV must contain 'danceability', 'tempo', and 'genre' columns")

    df["danceability"] = pd.to_numeric(df["danceability"], errors="coerce")
    df["tempo"] = pd.to_numeric(df["tempo"], errors="coerce")
    df["genre"] = df["genre"].fillna("Unknown")

    return df

# Store Uploaded CSV Info
@app.post("/upload/")
async def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    new_entry = UploadedCSV(filename=file.filename)
    db.add(new_entry)
    db.commit()
    return {"message": "File metadata saved", "filename": file.filename}

# Prediction Endpoint (Batch)
@app.post("/predict/")
async def predict(
    file: UploadFile = File(...),
    danceability_min: float = Query(...),
    danceability_max: float = Query(...),
    tempo_min: float = Query(...),
    tempo_max: float = Query(...),
    genre: str = Query("All"),
    db: Session = Depends(get_db),
):
    df = process_csv(file)

    filtered_df = df[
        (df["danceability"] >= danceability_min) & 
        (df["danceability"] <= danceability_max) &
        (df["tempo"] >= tempo_min) &
        (df["tempo"] <= tempo_max)
    ]

    if genre.lower() != "all":
        filtered_df = filtered_df[filtered_df["genre"].str.contains(genre, case=False, na=False)]

    if filtered_df.empty:
        return {"error": "No data found after applying filters"}

    results = filtered_df.to_dict(orient="records")

    try:
        new_entries = [
            DataEntry(
                danceability=row["danceability"],
                tempo=row["tempo"],
                genre_prediction=row["genre"],
                timestamp=datetime.datetime.utcnow()
            )
            for _, row in filtered_df.iterrows()
        ]

        if new_entries:
            db.add_all(new_entries)
            db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return {"message": "Filtered predictions", "results": results}

# Single Prediction Endpoint
@app.post("/single-predict/")
async def single_predict(danceability: float, tempo: float, genre: str, db: Session = Depends(get_db)):
    prediction_result = f"Predicted for {genre} with {danceability} danceability and {tempo} tempo"

    try:
        new_entry = DataEntry(danceability=danceability, tempo=tempo, genre_prediction=genre, timestamp=datetime.datetime.utcnow())
        db.add(new_entry)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return {"prediction": prediction_result}

# Retrieve Past Predictions
@app.get("/past-predictions/")
def get_past_predictions(start_date: str = None, end_date: str = None, db: Session = Depends(get_db)):
    query = db.query(DataEntry)

    if start_date:
        query = query.filter(DataEntry.timestamp >= datetime.datetime.strptime(start_date, "%Y-%m-%d"))
    if end_date:
        query = query.filter(DataEntry.timestamp <= datetime.datetime.strptime(end_date, "%Y-%m-%d"))

    predictions = query.all()

    return {"past_predictions": [
        {"id": p.id, "danceability": p.danceability, "tempo": p.tempo, "genre_prediction": p.genre_prediction, "timestamp": p.timestamp.strftime("%Y-%m-%d %H:%M:%S")}
        for p in predictions
    ]}
