from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import pandas as pd
import os
from database import SessionLocal, engine
import models

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Dummy prediction function
def generate_dummy_prediction(row):
    return "Hit" if row["feature1"] > 50 else "Flop"

# File path configuration
FILE_PATH = r"F:\EPITA - M.Sc CS\S2 - DSA\DSP\FastAPI_Song_Prediction\Music-Prediction-App\songs_normalize.csv"  # <-- PREDEFINED FILE PATH

@app.get("/process-local-file/")
async def process_local_file(db: Session = Depends(get_db)):
    try:
        df = pd.read_csv(FILE_PATH)
    except FileNotFoundError:
        return {"error": "File not found at the given path."}
    except Exception as e:
        return {"error": str(e)}

    # Check required columns
    if not {"feature1", "feature2"}.issubset(df.columns):
        return {"error": "CSV must contain 'feature1' and 'feature2' columns"}

    entries = []
    for _, row in df.iterrows():
        prediction = generate_dummy_prediction(row)
        entry = models.DataEntry(feature1=row["feature1"], feature2=row["feature2"], prediction=prediction)
        db.add(entry)
        entries.append({"feature1": row["feature1"], "feature2": row["feature2"], "prediction": prediction})

    db.commit()
    return {"message": "Local file processed successfully", "results": entries}
@app.post("/upload/")
async def process_local_file(db: Session = Depends(get_db)):
    df = pd.read_csv(FILE_PATH)

    # Validate columns
    if not {"feature1", "feature2"}.issubset(df.columns):
        return {"error": "CSV must contain 'feature1' and 'feature2' columns"}

    entries = []
    for _, row in df.iterrows():
        prediction = generate_dummy_prediction(row)  # Generate dummy result
        entry = models.DataEntry(feature1=row["feature1"], feature2=row["feature2"], prediction=prediction)

        db.add(entry)  # Store in PostgreSQL
        entries.append({"feature1": row["feature1"], "feature2": row["feature2"], "prediction": prediction})

    db.commit()  # Save all entries in DB
    return {"message": "Data stored", "results": entries}

@app.get("/results/")
async def get_results(db: Session = Depends(get_db)):
    results = db.query(models.DataEntry).all()
    return results 