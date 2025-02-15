from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()  # <-- This is necessary!

class DataEntry(Base):
    __tablename__ = "data_entries"

    id = Column(Integer, primary_key=True, index=True)
    feature1 = Column(Float, nullable=False)
    feature2 = Column(Float, nullable=False)
    prediction = Column(String, nullable=False)
