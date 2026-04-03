import os
import json
import glob
import re
from datetime import datetime
import pymysql
from typing import List, Dict, Any
from database_utils import get_db_config, get_connection

DB_CONFIG = get_db_config()

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
    """Crée la base de données et les tables normalisées si elles n'existent pas."""
    config = get_db_config()
    conn = pymysql.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"]
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cursor.execute(f"USE {DB_CONFIG['database']}")
            
            # Migration: On vérifie si l'ancienne structure existe
            cursor.execute("SHOW TABLES LIKE 'scraped_products'")
            if cursor.fetchone():
                cursor.execute("SHOW COLUMNS FROM scraped_products LIKE 'date_extraction'")
                if cursor.fetchone():
                    print("⚠️ Ancienne structure détectée. Migration vers le nouveau schéma...")
                    cursor.execute("DROP TABLE IF EXISTS product_scores") # Dépendance FK
                    cursor.execute("DROP TABLE IF EXISTS scraped_products")
            
            # 1. Table des Sessions
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date_session DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 2. Table des Produits (Métadonnées uniques)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraped_products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                boutique VARCHAR(100),
                categorie VARCHAR(100),
                nom TEXT,
                description TEXT,
                lien TEXT,
                product_id VARCHAR(100),
                UNIQUE KEY unique_prod_url (lien(255))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # 3. Table des Scores et Historique
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_scores (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT,
                session_id INT,
                prix_original VARCHAR(100),
                prix_usd FLOAT,
                note_etoiles FLOAT,
                nombre_avis INT,
                stock VARCHAR(100),
                score FLOAT DEFAULT NULL,
                FOREIGN KEY (product_id) REFERENCES scraped_products(id) ON DELETE CASCADE,
                FOREIGN KEY (session_id) REFERENCES scraping_sessions(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
        conn.commit()
        print(f"🗄️ Structure de base de données normalisée prête dans '{DB_CONFIG['database']}'.")
    finally:
        conn.close()

def save_to_mysql(products: List[Dict[str, Any]]):
    """Insère les produits dans la base de données MySQL avec gestion des sessions."""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            # 1. Création de la session
            cursor.execute("INSERT INTO scraping_sessions (date_session) VALUES (%s)", (datetime.now(),))
            session_id = cursor.lastrowid
            
            for prod in products:
                # 2. Upsert dans scraped_products
                upsert_product_query = """
                INSERT INTO scraped_products (boutique, categorie, nom, description, lien, product_id)
                VALUES (%(boutique)s, %(categorie)s, %(nom)s, %(description)s, %(lien)s, %(product_id)s)
                ON DUPLICATE KEY UPDATE 
                    nom = VALUES(nom), 
                    description = VALUES(description), 
                    product_id = VALUES(product_id),
                    categorie = VALUES(categorie)
                """
                cursor.execute(upsert_product_query, prod)
                
                # Récupération de l'ID du produit
                cursor.execute("SELECT id FROM scraped_products WHERE lien = %s", (prod['lien'],))
                db_product_id = cursor.fetchone()['id']
                
                # 3. Insertion dans product_scores
                score_query = """
                INSERT INTO product_scores (product_id, session_id, prix_original, prix_usd, note_etoiles, nombre_avis, stock)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(score_query, (
                    db_product_id, 
                    session_id, 
                    prod['prix_original'], 
                    prod['prix_usd'], 
                    prod.get('note_etoiles'), 
                    prod.get('nombre_avis'), 
                    prod.get('stock')
                ))
                
        conn.commit()
        print(f"🚀 {len(products)} produits synchronisés (Session ID: {session_id}).")
    except Exception as e:
        conn.rollback()
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
