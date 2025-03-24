from sqlalchemy import Column, Integer, String, Float, DateTime
from database import Base
import datetime

class DataEntry(Base):
    __tablename__ = "data_entries"

    id = Column(Integer, primary_key=True, index=True)
    danceability = Column(Float, nullable=False)
    tempo = Column(Float, nullable=False)
    genre_prediction = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class UploadedCSV(Base):
    __tablename__ = "uploaded_csv"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)