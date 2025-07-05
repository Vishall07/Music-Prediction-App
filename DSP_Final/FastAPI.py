
import datetime
import logging
from typing import List, Union

from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
import pandas as pd
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from database.models import (
    SinglePopularityPrediction,
    SimilarSongsPrediction,
    MultiplePredictions
)
from database import SessionLocal, get_db
from database.healthcheck import check_db_health

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(_name_)

MODEL_PATH = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/models/model.pkl"
try:
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    logger.info("Machine learning model loaded successfully")
except Exception as e:
    logger.error(f"Model loading failed: {str(e)}")
    raise RuntimeError("Could not initialize ML model")

app = FastAPI(
    title="Spotify Popularity Predictor",
    description="API for predicting song popularity and finding similar songs",
    version="1.0.0"
)

class PredictionRequest(BaseModel):
    """Requesting schema for single prediction"""
    song: str
    duration_ms: int
    year: int
    energy: float
    loudness: float
    genre: str

class MultiPredictionResponse(BaseModel):
    """Response schema for batch predictions"""
    batch_id: int
    total_predictions: int
    average_popularity: float
    predictions: dict
    """Predictions details"""

@app.post("/predict", response_model=Union[List[dict], dict])
async def make_prediction(request: Union[PredictionRequest, List[PredictionRequest]]):
    """
    Endpoint for making popularity predictions
    
    Args:
        request: Single prediction request or list of requests
        
    Returns:
        Prediction results with IDs
    """
    try:
        if not request:
            raise HTTPException(status_code=400, detail="Empty request received")
        
        requests = [request] if isinstance(request, PredictionRequest) else request
        predictions = []
        
        for req in requests:
            db = SessionLocal()
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
        logger.error(f"Prediction failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/similar-songs")
async def get_similar_songs(artist: str = None, song: str = None):
    """
    Find songs similar to given artist or track
    
    Args:
        artist: Artist name to match
        song: Song title to match
        
    Returns:
        List of similar songs with similarity scores
    """
    try:
        DATA_PATH = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/datasets/spotify_dataset.csv"
        df = pd.read_csv(DATA_PATH)
        
        if not artist and not song:
            return {"message": "Please provide artist or song name"}
        
        filtered_df = df.copy()
        if song:
            filtered_df = filtered_df[filtered_df['song'].str.lower().str.contains(song.lower(), na=False)]
        if artist:
            filtered_df = filtered_df[filtered_df['artist'].str.lower().str.contains(artist.lower(), na=False)]
        
        if filtered_df.empty:
            return {"message": "No matching songs found"}
        
        features = ['duration_ms', 'year', 'energy', 'loudness']
        genre_encoder = OneHotEncoder(handle_unknown='ignore')
        genres_encoded = genre_encoder.fit_transform(filtered_df[['genre']]).toarray()
        
        scaler = StandardScaler()
        numerical_features = scaler.fit_transform(filtered_df[features])
        all_features = np.hstack([numerical_features, genres_encoded])
        
        similarity_matrix = cosine_similarity(all_features)
        similar_indices = list(set(
            idx for i in range(len(similarity_matrix)) 
            for idx in np.argsort(similarity_matrix[i])[::-1][1:6]
        ))
        
        similar_songs = filtered_df.iloc[similar_indices].copy()
        similar_songs['similarity_score'] = [np.mean(similarity_matrix[idx]) for idx in similar_indices]
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
            
            return {
                "prediction_id": similar_pred.prediction_id,
                "reference": {"artist": artist, "song": song},
                "similar_songs": similar_songs_list
            }
            
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error: {str(e)}")
            raise HTTPException(500, detail="Failed to save prediction")
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Similar songs error: {str(e)}")
        raise HTTPException(500, detail=str(e))


@app.post("/multiple-predict", response_model=MultiPredictionResponse)
async def multiple_predict(file: UploadFile = File(...)):
    """
    Process batch predictions from CSV file
    
    Args:
        file: CSV file containing prediction data
        
    Returns:
        Batch prediction summary
    """
    db = SessionLocal()
    try:
        df = pd.read_csv(file.file)
        required_cols = ["song", "duration_ms", "year", "energy", "loudness", "genre"]
        
        if any(col not in df.columns for col in required_cols):
            raise HTTPException(400, detail=f"Missing columns: {required_cols}")

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
            "average_popularity": batch.average_popularity,
            "predictions": {
                "songs": df_clean['song'].tolist(),
                "predictions": predictions,
                "features": df_clean.to_dict('records')
            }
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Batch prediction failed: {str(e)}")
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
    Retrieve historical predictions
    
    Args:
        prediction_type: Type of predictions to retrieve
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        source: Prediction source filter
        
    Returns:
        List of matching predictions
    """
    try:
        db = SessionLocal()
        
        try:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(400, detail="Invalid date format. Use YYYY-MM-DD")
        
        if prediction_type == "single":
            model = SinglePopularityPrediction
            columns = ["prediction_id", "song", "duration_ms", "year", "energy", 
                    "loudness", "genre", "predicted_popularity", "timestamp"]
        elif prediction_type == "similar":
            model = SimilarSongsPrediction
            columns = ["prediction_id", "reference_song", "reference_artist", 
                    "similar_songs", "timestamp"]
        elif prediction_type == "multiple":
            model = MultiplePredictions
            columns = ["prediction_id", "filename", "total_predictions", 
                    "average_popularity", "timestamp"]
        else:
            raise HTTPException(400, detail="Invalid prediction type")
        
        query = db.query(model).filter(
            model.timestamp >= start_date,
            model.timestamp <= end_date + datetime.timedelta(days=1)
        )
        
        if source != "ALL":
            query = query.filter(model.source == source)
            
        if not (results := query.all()):
            return {"message": "No predictions found"}
            
        return [
            {col: getattr(r, col) for col in columns if hasattr(r, col)}
            for r in results
        ]
        
    except Exception as e:
        logger.error(f"History Retrieval failed: {str(e)}")
        raise HTTPException(500, detail=str(e))
    finally:
        db.close()
