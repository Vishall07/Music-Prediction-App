from fastapi import FastAPI, Depends, Query, UploadFile, File
from sqlalchemy.orm import Session
import pandas as pd
import io
import models
from database import SessionLocal, engine
import datetime

# Create tables in the database
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Store uploaded file
uploaded_file_path = "uploaded_data.csv"

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    contents = await file.read()
    with open(uploaded_file_path, "wb") as f:
        f.write(contents)
    return {"message": "File uploaded successfully"}

@app.get("/predict/")
async def predict(
    danceability_min: float = Query(..., description="Minimum danceability"),
    danceability_max: float = Query(..., description="Maximum danceability"),
    tempo_min: float = Query(..., description="Minimum tempo"),
    tempo_max: float = Query(..., description="Maximum tempo"),
    genre: str = Query("All", description="Genre to filter"),
    db: Session = Depends(get_db),
):
    try:
        # Read the uploaded CSV file
        df = pd.read_csv(uploaded_file_path)
    except FileNotFoundError:
        return {"error": "File not found. Please upload a file first."}
    except Exception as e:
        return {"error": str(e)}

    # Validate required columns
    if not {"danceability", "tempo", "genre"}.issubset(df.columns):
        return {"error": "CSV must contain 'danceability', 'tempo', and 'genre' columns"}

    # Ensure the data columns are numeric
    df["danceability"] = pd.to_numeric(df["danceability"], errors="coerce")
    df["tempo"] = pd.to_numeric(df["tempo"], errors="coerce")
    df["genre"] = df["genre"].fillna("Unknown")  # Handle missing genres

    # Apply filters
    filtered_df = df[
        (df["danceability"] >= danceability_min) &
        (df["danceability"] <= danceability_max) &
        (df["tempo"] >= tempo_min) &
        (df["tempo"] <= tempo_max)
    ]

    if genre.lower() != "all":
        filtered_df = filtered_df[filtered_df["genre"].str.contains(genre, case=False, na=False)]

    # Print the filtered data to check
    print(f"Filtered data:\n{filtered_df}")

    # Check if filtered dataframe is empty
    if filtered_df.empty:
        return {"error": "No data found after applying the filters."}

    # Convert filtered data to a dictionary
    results = filtered_df.to_dict(orient="records")
    return {"message": "Filtered predictions", "results": results}