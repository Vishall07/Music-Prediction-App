from fastapi import FastAPI, HTTPException, UploadFile, File
from typing import List, Union
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from database.models import (
    SinglePopularityPrediction, 
    SimilarSongsPrediction,
    MultiplePredictions,
    Base
)
from database.init_db import SessionLocal, engine
import pickle
import pandas as pd
import datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import numpy as np
from sqlalchemy.exc import SQLAlchemyError
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URI = "postgresql+psycopg2://postgres:dragonhont@localhost:5432/spotify_ml"
engine = create_engine(DATABASE_URI)

Base.metadata.create_all(bind=engine)

try:
    with open("C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/models/model.pkl", "rb") as f:
        model = pickle.load(f)
    logger.info("Model loaded successfully")
except Exception as e:
    logger.error(f"Failed to load model: {str(e)}")
    raise

app = FastAPI()

class PredictionRequest(BaseModel):
    """Model for prediction request data"""
    song: str
    duration_ms: int
    year: int
    energy: float
    loudness: float
    genre: str

class MultiplPredictionResponse(BaseModel):
    multiple_id: int
    total_predictions: int
    average_popularity: float

@app.post("/predict")
async def make_prediction(request: Union[PredictionRequest, List[PredictionRequest]]):
    try:
        if not request:
            raise HTTPException(status_code=400, detail="Empty request received")

        requests = [request] if isinstance(request, PredictionRequest) else request
        predictions = []
        
        for req in requests:
            try:
                if not all([req.song, req.genre]):
                    continue

                input_data = pd.DataFrame([{
                    "duration_ms": req.duration_ms,
                    "year": req.year,
                    "energy": req.energy,
                    "loudness": req.loudness,
                    "genre": req.genre
                }])
                
                prediction_result = model.predict(input_data).tolist()[0]

                db = SessionLocal()
                db_prediction = SinglePopularityPrediction(
                    song=req.song,
                    duration_ms=req.duration_ms,
                    year=req.year,
                    energy=req.energy,
                    loudness=req.loudness,
                    genre=req.genre,
                    predicted_popularity=float(prediction_result),
                    source="WEBAPP"
                )
                db.add(db_prediction)
                db.commit()
                predictions.append({
                    "prediction": prediction_result,
                    "prediction_id": db_prediction.prediction_id
                })
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                continue
            finally:
                db.close()

        return predictions if len(predictions) > 1 else predictions[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/similar-songs")
async def get_similar_songs(artist: str = None, song: str = None):
    """
    Endpoint for finding similar songs
    """
    try:
        df = pd.read_csv("C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/datasets/spotify_dataset.csv")
        
        if not artist and not song:
            return {"message": "Please provide either artist name or song name"}
        
        filtered_df = df.copy()
        if song:
            filtered_df = filtered_df[filtered_df['song'].str.lower().str.contains(song.lower(), na=False)]
        if artist:
            filtered_df = filtered_df[filtered_df['artist'].str.lower().str.contains(artist.lower(), na=False)]
        
        if filtered_df.empty:
            return {"message": "No matching songs found. Please check your input."}
        
        features = ['duration_ms', 'year', 'energy', 'loudness']
        genre_encoder = OneHotEncoder(handle_unknown='ignore')
        genres_encoded = genre_encoder.fit_transform(filtered_df[['genre']]).toarray()
        
        scaler = StandardScaler()
        numerical_features = scaler.fit_transform(filtered_df[features])
        
        all_features = np.hstack([numerical_features, genres_encoded])
        similarity_matrix = cosine_similarity(all_features)
        
        similar_indices = []
        for i in range(len(similarity_matrix)):
            similar_indices.extend(np.argsort(similarity_matrix[i])[::-1][1:6])
        
        similar_indices = list(set(similar_indices))
        similar_songs = filtered_df.iloc[similar_indices].copy()
        
        similarity_scores = [np.mean(similarity_matrix[idx]) for idx in similar_indices]
        similar_songs['similarity_score'] = similarity_scores
        similar_songs = similar_songs.sort_values('similarity_score', ascending=False).head(5)
        
        similar_songs_list = similar_songs[['song', 'artist', 'similarity_score']].to_dict('records')
        
        db = SessionLocal()
        try:
            similar_pred = SimilarSongsPrediction(
                reference_song=song,
                reference_artist=artist,
                similar_songs=similar_songs_list,
                source="WEBAPP"
            )
            db.add(similar_pred)
            db.commit()
            db.refresh(similar_pred)
            
            return {
                "prediction_id": similar_pred.prediction_id,
                "reference": {"artist": artist, "song": song},
                "similar_songs": similar_songs_list
            }
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database Error: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to save similar songs prediction")
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Similar Songs Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/multiple-predict")
async def multiple_predict(file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        df = pd.read_csv(file.file)
        required_cols = ["song", "duration_ms", "year", "energy", "loudness", "genre"]
        
        if any(col not in df.columns for col in required_cols):
            raise HTTPException(400, detail=f"Missing required columns: {required_cols}")

        df_clean = df[required_cols].dropna()
        if df_clean.empty:
            raise HTTPException(400, detail="No valid data after cleaning")
        
        predictions = model.predict(pd.DataFrame(df_clean)).tolist()
        
        batch = MultiplePredictions(
            filename=file.filename,
            total_predictions=len(predictions),
            average_popularity=float(np.mean(predictions)),
            predictions={
                "songs": df_clean['song'].tolist(),
                "predictions": predictions,
                "features": df_clean.to_dict('records')
            },
            source="WEBAPP"
        )
        db.add(batch)
        db.commit()
        
        return {
            "batch_id": batch.prediction_id,
            "total_predictions": batch.total_predictions,
            "average_popularity": batch.average_popularity
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(500, detail=str(e))
    finally:
        db.close()
    
    
@app.get("/past-predictions/{prediction_type}")
async def get_past_predictions(
    prediction_type: str,
    start_date: str,
    end_date: str,
    source: str = "WEBAPP"
):
    """
    Endpoint for retrieving past predictions by type
    """
    try:
        db = SessionLocal()
        
        try:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        if prediction_type == "single":
            model = SinglePopularityPrediction
            columns = ["prediction_id", "song", "duration_ms", "year", "energy", "loudness", "genre", "predicted_popularity", "timestamp"]
        elif prediction_type == "similar":
            model = SimilarSongsPrediction
            columns = ["prediction_id", "reference_song", "reference_artist", "similar_songs", "timestamp"]
        elif prediction_type == "multiple":
            model = MultiplePredictions
            columns = ["prediction_id", "filename", "total_predictions", "average_popularity", "timestamp"]
        else:
            raise HTTPException(status_code=400, detail="Invalid prediction type")
        
        query = db.query(model).filter(
            model.timestamp >= start_date,
            model.timestamp <= end_date + datetime.timedelta(days=1)
        )
        
        if source != "ALL":
            query = query.filter(model.source == source)
            
        if not (results := query.all()):
            return {"message": "No predictions found for the selected criteria"}
        else:
            
            return [
                {col: getattr(r, col) for col in columns if hasattr(r, col)}
                for r in results
            ]
        
    except Exception as e:
        logger.error(f"Past Predictions Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
