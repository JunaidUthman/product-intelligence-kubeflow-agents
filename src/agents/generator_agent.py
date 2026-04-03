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
            "nom_boutique": "Nothing Tech",
            "url": "https://eu.redmagic.gg/collections/smartphones",
            "platform": "shopify",
            "category": "phones"
        },
    
        # {
        #     "nom_boutique": "RedMagic",
        #     "url": "https://eu.redmagic.gg/collections/smartphones",
        #     "platform": "shopify",
        #     "category": "phones"
        # },

    #     # ==========================================
    #     # 2. ORDINATEURS (PCS)
    #     # ==========================================
        # {
        #     "nom_boutique": "XOTIC PC",
        #     "url": "https://xoticpc.com/collections/custom-gaming-laptops-notebooks",
        #     "platform": "shopify",
        #     "category": "pcs"
        # },
    

    #     # ==========================================
    #     # 3. CHARGEURS (CHARGERS)
    #     # ==========================================
        # {
        #     "nom_boutique": "Anker US",
        #     "url": "https://us.anker.com/collections/chargers",
        #     "platform": "shopify",
        #     "category": "chargers"
        # }
    ]
    
    print(f"{len(targets)} sous-catégories prêtes pour le traitement distribué.")
    
    return targets

# --- Test local ---
if __name__ == "__main__":
    cibles = get_scraping_targets()
    for cible in cibles:
        print(f"[{cible['platform'].upper()}] - {cible['nom_boutique']} : Catégorie '{cible['category']}' -> {cible['url']}")