import os
import pymysql
from database_utils import get_connection
from typing import List, Dict, Any

def get_latest_session_id(cursor):
    """Récupère l'ID de la dernière session de scraping."""
    cursor.execute("SELECT MAX(id) as id FROM scraping_sessions")
    result = cursor.fetchone()
    return result['id'] if result else None

def calculate_category_scores(cursor, session_id):
    """Calcule et met à jour les scores pour une session donnée."""
    # 1. Récupération des données par catégorie
    query = """
    SELECT 
        ps.id as score_entry_id, 
        p.categorie, 
        ps.prix_usd, 
        ps.note_etoiles, 
        ps.stock
    FROM product_scores ps
    JOIN scraped_products p ON ps.product_id = p.id
    WHERE ps.session_id = %s
    """
    cursor.execute(query, (session_id,))
    products = cursor.fetchall()
    
    if not products:
        print(f"⚠️ Aucun produit trouvé pour la session {session_id}.")
        return

    # Groupement par catégorie pour normalisation des prix
    categories_data = {}
    for p in products:
        cat = p['categorie']
        if cat not in categories_data:
            categories_data[cat] = []
        categories_data[cat].append(p)

    print(f"📊 Calcul des scores pour {len(categories_data)} catégories...")

    for cat, items in categories_data.items():
        # Trouver le prix max de la catégorie pour normaliser
        max_price = max([p['prix_usd'] for p in items if p['prix_usd']] or [1.0])
        
        for p in items:
            # --- FACTEUR 1 : Rating (40%) ---
            # On assume 4.1 par défaut si None
            rating = p['note_etoiles'] if p['note_etoiles'] is not None else 4.1
            norm_rating = rating / 5.0
            
            # --- FACTEUR 2 : Prix (35%) ---
            # Plus le prix est bas, plus le score est haut
            price = p['prix_usd'] if p['prix_usd'] else max_price
            norm_price = 1.0 - (price / max_price) if max_price > 0 else 1.0
            
            # --- FACTEUR 3 : Stock (25%) ---
            # Priorité aux produits en stock
            stock_status = str(p['stock'] or "").lower()
            stock_score = 1.0 if any(word in stock_status for word in ["in stock", "en stock", "available"]) else 0.0
            
            # SCORE FINAL
            final_score = (norm_rating * 0.4) + (norm_price * 0.35) + (stock_score * 0.25)
            
            # Mise à jour en base
            update_query = "UPDATE product_scores SET score = %s WHERE id = %s"
            cursor.execute(update_query, (final_score, p['score_entry_id']))

    print(f"✅ Scores mis à jour pour la session {session_id}.")

def run_ranking():
    """Point d'entrée de l'agent de ranking."""
    print("🤖 Agent Ranking (K-Top) : Calcul des scores en cours...")
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            session_id = get_latest_session_id(cursor)
            if session_id:
                calculate_category_scores(cursor, session_id)
                conn.commit()
            else:
                print("❌ Aucune session de scraping trouvée.")
    except Exception as e:
        print(f"❌ Erreur lors du ranking : {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_ranking()
