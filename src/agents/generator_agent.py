from typing import List, Dict

def get_scraping_targets() -> List[Dict[str, str]]:
    """
    Génère la liste des VRAIS sites e-commerce à scraper.
    Catégories ciblées : Téléphones, PCs, et Chargeurs.
    """
    
    print("Initialisation des cibles e-commerce (Téléphones, PCs, Chargeurs)...")
    
    targets = [
        # ==========================================
        # 1. TÉLÉPHONES (PHONES)
        # ==========================================
        {
            "nom_boutique": "Blackview",
            "url": "https://store.blackview.hk/collections/smartphones",
            "platform": "shopify",
            "category": "phones"
        },
        # ==========================================
        # 2. ORDINATEURS & MINI PCS (PCS)
        # ==========================================
        {
            "nom_boutique": "Techsavers",
            "url": "https://techsavers.com/collections/laptops",
            "platform": "shopify",
            "category": "pcs"
        },

        # ==========================================
        # 3. CHARGEURS & ALIMENTATION (CHARGERS)
        # ==========================================
        {
            "nom_boutique": "Rolling Square",
            "url": "https://rollingsquare.com/collections/all",
            "platform": "shopify",
            "category": "chargers"
        }
    ]
    
    print(f"{len(targets)} sous-catégories prêtes pour le traitement distribué.")
    
    return targets

# --- Test local ---
if __name__ == "__main__":
    cibles = get_scraping_targets()
    for cible in cibles:
        print(f"[{cible['platform'].upper()}] - {cible['nom_boutique']} : Catégorie '{cible['category']}' -> {cible['url']}")