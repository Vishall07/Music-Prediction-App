from sqlalchemy import text
from . import engine

def check_db_health():
    """Check if database is reachable"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            return True
    except Exception as e:
        print(f"Database health check failed: {e}")
        return False