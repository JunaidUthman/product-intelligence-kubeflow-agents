import os
import json
import glob
import pymysql
from dotenv import load_dotenv
from typing import List, Dict, Any
from datetime import datetime
import re

# Load environment variables from src/.env
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(dotenv_path=env_path)

# Configuration MySQL depuis .env
DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST"),
    "user": os.environ.get("MYSQL_USER"),
    "password": os.environ.get("MYSQL_PASSWORD"),
    "database": os.environ.get("MYSQL_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def normalize_price_to_usd(price_str: str) -> float:
    """Nettoie et convertit le prix en USD."""
    if not price_str:
        return 0.0
    
    # Extraction du nombre (gère 1,234.56 ou 1234,56)
    price_str = price_str.replace(',', '.')
    numbers = re.findall(r"(\d+\.?\d*)", price_str)
    if not numbers:
        return 0.0
    
    val = float(numbers[0])
    
    # Conversion basique
    if "£" in price_str or "GBP" in price_str.upper():
        return round(val * 1.25, 2)
    if "€" in price_str or "EUR" in price_str.upper():
        return round(val * 1.08, 2)
    
    return round(val, 2)

def clean_and_aggregate_data() -> List[Dict[str, Any]]:
    """Agrège tous les fichiers JSON, dédoublonne et nettoie les valeurs."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(os.path.dirname(project_root), "data")
    json_files = glob.glob(os.path.join(data_dir, "*.json"))
    
    all_products = []
    seen_links = set()
    
    print(f"📂 Lecture de {len(json_files)} fichiers JSON dans {data_dir}...")
    
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue
                
                for item in data:
                    link = item.get("lien")
                    # Dédoublonnage simple par lien pour cette session d'agrégation
                    if link in seen_links:
                        continue
                    seen_links.add(link)
                    
                    # Nettoyage et Valeurs Standards
                    cleaned_item = {
                        "boutique": item.get("boutique", "Inconnu"),
                        "categorie": item.get("categorie", "divers"),
                        "nom": item.get("nom", "Sans Nom"),
                        "prix_original": item.get("prix", "0"),
                        "prix_usd": normalize_price_to_usd(item.get("prix", "0")),
                        "description": item.get("description", ""),
                        "note_etoiles": item.get("note_etoiles") if item.get("note_etoiles") is not None else 4.1,
                        "nombre_avis": item.get("nombre_avis") if item.get("nombre_avis") is not None else 0,
                        "stock": item.get("stock", "Unknown"),
                        "lien": link,
                        "product_id": item.get("product_id", "N/A"),
                        "date_extraction": item.get("date_extraction")
                    }
                    all_products.append(cleaned_item)
        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture de {file_path} : {e}")
            
    print(f"✅ Agrégation terminée : {len(all_products)} produits uniques trouvés.")
    return all_products

def setup_database():
    """Crée la base de données et la table si elles n'existent pas."""
    # Connexion sans base de données pour la création
    conn = pymysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cursor.execute(f"USE {DB_CONFIG['database']}")
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS scraped_products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                boutique VARCHAR(100),
                categorie VARCHAR(100),
                nom TEXT,
                prix_original VARCHAR(100),
                prix_usd FLOAT,
                description TEXT,
                note_etoiles FLOAT,
                nombre_avis INT,
                stock VARCHAR(100),
                lien TEXT,
                product_id VARCHAR(100),
                date_extraction DATETIME,
                date_aggregation DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_prod_snapshot (lien(255), date_extraction)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_query)
        conn.commit()
        print(f"🗄️ Base de données '{DB_CONFIG['database']}' prête.")
    finally:
        conn.close()

def save_to_mysql(products: List[Dict[str, Any]]):
    """Insère les produits dans la base de données MySQL."""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT IGNORE INTO scraped_products 
            (boutique, categorie, nom, prix_original, prix_usd, description, note_etoiles, nombre_avis, stock, lien, product_id, date_extraction)
            VALUES (%(boutique)s, %(categorie)s, %(nom)s, %(prix_original)s, %(prix_usd)s, %(description)s, %(note_etoiles)s, %(nombre_avis)s, %(stock)s, %(lien)s, %(product_id)s, %(date_extraction)s)
            """
            cursor.executemany(insert_query, products)
        conn.commit()
        print(f"🚀 {len(products)} produits synchronisés avec MySQL.")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion MySQL : {e}")
    finally:
        conn.close()

def run_processor():
    """Lancement du cycle complet de traitement."""
    print("🤖 Agent Processeur : Démarrage du cycle d'agrégation...")
    setup_database()
    products = clean_and_aggregate_data()
    if products:
        save_to_mysql(products)
    print("🏁 Cycle de traitement terminé.")

if __name__ == "__main__":
    run_processor()
