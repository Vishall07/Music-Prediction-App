#---------------------------------------------------------------------------------------------------------------#
#                                   Database Configurations (Environment Variables)
#---------------------------------------------------------------------------------------------------------------#

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

#-------------------------------------------- Database Configuartion -------------------------------------------#
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "name": os.getenv("DB_NAME")
}

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
    f"{DB_CONFIG['name']}"
)
engine = create_engine(
    SQLALCHEMY_DATABASE_URI,
    pool_size=20,
    max_overflow=0,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "connect_timeout": 5,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionAirflow = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Database dependency for FastAPI"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Initialize database tables"""
    from .models import Base
    Base.metadata.create_all(bind=engine)

def init_airflow_db():
    """Initialize database tables for Airflow database"""
    from .models import Base
    try:
        # Use explicit Airflow database configuration
        DB_CONFIG = {
            "user": "",
            "password": "",
            "host": "",
            "port": "5432",
            "name": ""
        }
        
        SQLALCHEMY_DATABASE_URI = (
            f"postgresql+psycopg2://"
            f"{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
            f"{DB_CONFIG['name']}"
        )
        
        engine = create_engine(SQLALCHEMY_DATABASE_URI)
        Base.metadata.create_all(bind=engine)
        print("Airflow database tables initialized successfully")
    except Exception as e:
        print(f"Error initializing Airflow database: {e}")
