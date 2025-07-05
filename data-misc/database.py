from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define your PostgreSQL database URL
DATABASE_URL = "postgresql://postgres:sql.123@localhost:5432/fastapi_db"  # Change this

# Create the engine
engine = create_engine(DATABASE_URL)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Define Base
Base = declarative_base()