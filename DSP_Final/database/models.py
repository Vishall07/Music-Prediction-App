from sqlalchemy import Column, Integer, String, JSON, DateTime, Float, Boolean, Text, CheckConstraint, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
import datetime
from enum import Enum
from . import Base

class PredictionType(Enum):
    SINGLE_POPULARITY = "single_popularity"
    SIMILAR_SONGS = "similar_songs"
    MULTIPLE_PREDICTION = "multipl_prediction"

class BasePrediction(Base):
    __abstract__ = True
    prediction_id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    source = Column(String(20), default="WEBAPP")

class SinglePopularityPrediction(BasePrediction):
    __tablename__ = "single_popularity_predictions"
    song = Column(String(255), nullable=False)
    duration_ms = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    energy = Column(Float, nullable=False)
    loudness = Column(Float, nullable=False)
    genre = Column(String(100), nullable=False)
    predicted_popularity = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    source = Column(String(20), nullable=False)

    __table_args__ = (
        CheckConstraint('song ~ \'^[a-zA-Z ]+$\'', name='song_letters_only'),
    )

class SimilarSongsPrediction(BasePrediction):
    __tablename__ = "similar_songs_predictions"
    reference_song = Column(String(255))
    reference_artist = Column(String(255))
    similar_songs = Column(JSON, nullable=False)

class MultiplePredictions(BasePrediction):
    __tablename__ = "multiple_predictions"
    filename = Column(String(255))
    total_predictions = Column(Integer)
    average_popularity = Column(Float)
    predictions = Column(JSON)

class DataQualityStat(Base):
    __tablename__ = "data_quality_stats"
    stat_id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    total_rows = Column(Integer, nullable=False)
    valid_rows = Column(Integer, nullable=False)
    invalid_rows = Column(Integer, nullable=False)
    error_counts = Column(JSON, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class ProcessedFile(Base):
    __tablename__ = "processed_files"
    file_id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), unique=True, nullable=False)
    processed_at = Column(DateTime, default=datetime.datetime.utcnow)

class TrainingStat(Base):
    __tablename__ = "training_stats"
    stat_id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(255), nullable=False)
    mean = Column(Float)
    std = Column(Float)
    distribution = Column(JSON)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    
class ValidationResult(Base):
    __tablename__ = "validation_results"
    id = Column(Integer, primary_key=True, autoincrement=True)
    expectation_type = Column(String(100), nullable=False)
    column_name = Column(String(100))
    error_message = Column(Text)
    validation_timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class IngestedData(Base):
    __tablename__ = "ingested_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    artist = Column(String(255))
    song = Column(String(255))
    duration_ms = Column(Integer)
    explicit = Column(Boolean)
    year = Column(Integer)
    popularity = Column(Integer)
    danceability = Column(Float)
    energy = Column(Float)
    key = Column(Integer)
    loudness = Column(Float)
    mode = Column(Integer)
    speechiness = Column(Float)
    acousticness = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)
    valence = Column(Float)
    tempo = Column(Float)
    genre = Column(String(100))
    ingestion_timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class ScheduledPrediction(Base):
    __tablename__ = "scheduled_predictions"

    prediction_id = Column(Integer, primary_key=True, autoincrement=True)
    song = Column(Text, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    energy = Column(Float, nullable=False)
    loudness = Column(Float, nullable=False)
    genre = Column(Text, nullable=False)
    predicted_popularity = Column(Float, nullable=False)
    processed_at = Column(TIMESTAMP(timezone=False), server_default=func.current_timestamp(), nullable=True)
    
    @property
    def timestamp(self):
        return self.processed_at