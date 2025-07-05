import os
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import psycopg2
from datetime import datetime
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        dbname=os.getenv("DB_NAME")
    )

# Features used for similarity
features = ['danceability','energy','loudness','speechiness','acousticness',
            'instrumentalness','liveness','valence','tempo','popularity']

# Load and scale data
songs_df = pd.read_csv("songs.csv")
songs_df[features] = songs_df[features].fillna(0)
scaler = MinMaxScaler()
scaled_features = scaler.fit_transform(songs_df[features])
similarity_matrix = cosine_similarity(scaled_features)

# Predict similar song
def recommend(song_name: str):
    if song_name not in songs_df["song"].values:
        return None
    idx = songs_df[songs_df["song"] == song_name].index[0]
    similarities = similarity_matrix[idx]
    # Exclude the song itself
    similarities[idx] = -1
    best_idx = np.argmax(similarities)
    rec_row = songs_df.iloc[best_idx]
    return {
        "recommended_song": rec_row["song"],
        "artist": rec_row["artist"],
        "genre": rec_row["genre"]
    }

@app.post("/predict")
def predict(
    songs: list[str] = Body(..., example=["songA", "songB"]),
    request_source: str = Query(..., example="webapp")
):
    if not songs:
        raise HTTPException(status_code=400, detail="Songs list cannot be empty.")
    result = {}
    for s in songs:
        rec = recommend(s)
        if rec is None:
            result[s] = {"error": "Song not found"}
        else:
            result[s] = rec

    # Store predictions
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO predictions (timestamp, request_source, input_songs, output_predictions)
        VALUES (%s, %s, %s, %s)
        """,
        (
            datetime.utcnow(),
            request_source,
            json.dumps(songs),
            json.dumps(result)
        )
    )
    conn.commit()
    cur.close()
    conn.close()
    return result

@app.get("/past-predictions")
def past_predictions(
    limit: int = Query(100, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, request_source, input_songs, output_predictions
        FROM predictions
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """,
        (limit, offset)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "timestamp": r[0].isoformat(),
            "request_source": r[1],
            "input_songs": r[2],           # No json.loads()
            "output_predictions": r[3]     # No json.loads()
        }
        for r in rows
    ]
    
@app.get("/songs")
def get_songs(
    limit: int = 10,
    offset: int = 0
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            artist,
            song,
            year,
            popularity,
            genre
        FROM songs
        ORDER BY popularity DESC
        LIMIT %s OFFSET %s
        """,
        (limit, offset)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "artist": r[0],
            "song": r[1],
            "year": r[2],
            "popularity": r[3],
            "genre": r[4]
        }
        for r in rows
    ]
