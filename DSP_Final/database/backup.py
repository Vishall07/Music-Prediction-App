import subprocess
from datetime import datetime
from urllib.parse import urlparse
from . import SQLALCHEMY_DATABASE_URI
import os

def backup_db(backup_dir="backups"):
    """Create database backup"""
    os.makedirs(backup_dir, exist_ok=True)
    
    db_url = urlparse(SQLALCHEMY_DATABASE_URI)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f"backup_{timestamp}.sql")
    
    command = [
        "pg_dump",
        "-h", db_url.hostname,
        "-p", str(db_url.port),
        "-U", db_url.username,
        "-d", db_url.path[1:],
        "-f", backup_file,
        "-F", "c",  # Custom format
        "-w"  # No password prompt
    ]
    
    try:
        subprocess.run(
            command,
            env={"PGPASSWORD": db_url.password},
            check=True
        )
        return backup_file
    except subprocess.CalledProcessError as e:
        print(f"Backup failed: {e}")
        return None