import os
import pymysql
from dotenv import load_dotenv
from typing import List, Dict, Any

def get_db_config():
    """Charge la configuration DB depuis le .env."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(dotenv_path=env_path)
    
    return {
        "host": os.environ.get("MYSQL_HOST"),
        "user": os.environ.get("MYSQL_USER"),
        "password": os.environ.get("MYSQL_PASSWORD"),
        "database": os.environ.get("MYSQL_DATABASE"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor
    }

def get_connection():
    """Crée une connexion MySQL."""
    return pymysql.connect(**get_db_config())
