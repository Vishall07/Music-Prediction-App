from sqlalchemy import Column, Integer, String, Float, DateTime
from database import Base  # Ensure you're using Base from database.py
import datetime

class DataEntry(Base):
    __tablename__ = "data_entries"

    id = Column(Integer, primary_key=True, index=True)
    feature1 = Column(Float, nullable=False)
    feature2 = Column(Float, nullable=False)
    prediction = Column(String, nullable=False)
    date = Column(DateTime, default=datetime.datetime.utcnow)