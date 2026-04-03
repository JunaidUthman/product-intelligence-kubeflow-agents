import os
import json
import asyncio
import httpx
import datetime
import argparse
import urllib.parse
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Response
from openai import AsyncOpenAI
from dotenv import load_dotenv
from typing import List, Dict, Any

from generator_agent import get_scraping_targets

# Load environment variables from src/.env
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(dotenv_path=env_path)

# Instanciation de DeepSeek en asynchrone (OpenAI SDK)
deepseek_client = AsyncOpenAI(
    api_key=os.environ.get("DEEPSEEK_API_KEY"),
    base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
)

async def call_deepseek(prompt: str) -> dict:
    """Fonction utilitaire pour appeler DeepSeek et récupérer un JSON strict."""
    try:
        response = await deepseek_client.chat.completions.create(
            model="deepseek-chat",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system", 
                    "content": "Tu es un ingénieur MLOps expert en Web Scraping. "
                               "Tu dois TOUJOURS répondre UNIQUEMENT avec un objet JSON valide, "
                               "correspondant très exactement aux clés demandées par le prompt."
                },
                {"role": "user", "content": prompt},
            ],
            stream=False
        )
        content = response.choices[0].message.content.strip()
        return json.loads(content)
    except Exception as e:
        print(f"❌ Erreur DeepSeek : {e}")
        return {}

def extract_json_ld(soup: BeautifulSoup) -> dict:
    """Extrait les données structurées JSON-LD de manière agressive."""
    data = {}
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            if not script.string: continue
            js = json.loads(script.string.strip())
            
            # Normalisation en liste d'objets
            items = js if isinstance(js, list) else (js.get('@graph', [js]) if isinstance(js, dict) else [])
            
            for item in items:
                if not isinstance(item, dict): continue
                ptype = item.get('@type', [])
                types = ptype if isinstance(ptype, list) else [ptype]
                
                if any(t in ['Product', 'http://schema.org/Product'] for t in types):
                    data['name'] = item.get('name')
                    data['description'] = item.get('description')
                    
                    # Extraction des notes (ratings)
                    rating = item.get('aggregateRating')
                    if isinstance(rating, dict):
                        data['stars'] = rating.get('ratingValue')
                        data['reviews_count'] = rating.get('reviewCount')
                    
                    offers = item.get('offers', {})
                    if isinstance(offers, list) and offers:
                        offers = offers[0]
                    # Extraction du prix
                    price_val = offers.get('price')
                    currency = offers.get('priceCurrency', '')
                    if price_val:
                        data['price'] = f"{price_val} {currency}".strip()
                    # Stock / Disponibilité
                    availability = offers.get('availability')
                    if availability:
                        data['stock'] = "In Stock" if "InStock" in availability else "Out of Stock"
                    # ID produit
                    data['product_id'] = item.get('sku') or item.get('productID') or item.get('mpn')
                    return data
        except Exception:
            continue
    return data

def extract_meta_tags(soup: BeautifulSoup) -> dict:
    """Fallback ultime via les Meta Tags SEO (OpenGraph, etc.)."""
    data = {}
    def get_meta(prop_or_name):
        tag = soup.find('meta', property=prop_or_name) or soup.find('meta', attrs={"name": prop_or_name})
        return tag.get('content') if tag else None

    data['name'] = get_meta('og:title') or get_meta('twitter:title') or (soup.title.string if soup.title else None)
    data['description'] = get_meta('og:description') or get_meta('description')
    data['price'] = get_meta('product:price:amount')
    data['product_id'] = get_meta('product:reference') or get_meta('og:product:group_id')
    data['stock'] = get_meta('og:availability') or get_meta('product:availability')
    
    currency = get_meta('product:price:currency')
    if data['price'] and currency:
        data['price'] = f"{data['price']} {currency}"
    return data

def clean_html_for_llm(html_content: str, max_chars: int = 80000) -> str:
    """Nettoie le HTML pour n'avoir que l'essentiel et réduire les tokens."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Suppression des balises inutiles
    for tag in soup(['script', 'style', 'noscript', 'svg', 'img', 'video', 'iframe', 'link', 'meta']):
        # On garde JSON-LD car il contient souvent toutes les infos (Price, SKU, Name)
        if tag.name == 'script' and tag.get('type') == 'application/ld+json':
            continue
        tag.decompose()
        
    # Retirer les attributs très longs mais inutiles pour le scraping
    for tag in soup.find_all(True):
        for attr in ['style', 'd', 'srcset', 'sizes']:
            if attr in tag.attrs:
                del tag.attrs[attr]
            
    # Ciblage prioritaire du contenu principal
    main_content = soup.find('main') or soup.find(id='MainContent') or soup.find(id='content') or soup.find(itemtype="http://schema.org/Product")
    if main_content:
        content_str = str(main_content)
    else:
        body = soup.find('body')
        content_str = str(body) if body else str(soup)
        
    return content_str[:max_chars]

async def extract_catalog_links(page: Page, target: dict) -> List[str]:
    """ÉTAPE 1 : Extraction des URLs des PDP depuis la page catalogue."""
    print(f"\n🕵️ ÉTAPE 1 : Extraction du Catalogue -> {target['url']}")
    
    await page.goto(target["url"], wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(3000) # Laisser le temps au JS d'insérer les liens
    html_content = await page.content()
    clean_html = clean_html_for_llm(html_content, max_chars=40000)
    
    prompt = f"""
    Voici le HTML nettoyé d'une page catalogue de produits e-commerce ({target['url']}).
    Identifie le sélecteur CSS exact pour trouver les liens (balises <a>) qui mènent aux pages de détails approfondies (PDP - Product Detail Page) ou pages "See More" des produits.
    Ne prends pas les liens hors-sujet. 
    Renvoie un JSON de ce format exact :
    {{
        "product_link_selector": "sélecteur css exact pour <a>"
    }}
    HTML:
    {clean_html}
    """
    
    selectors = await call_deepseek(prompt)
    link_selector = selectors.get("product_link_selector")
    
    if not link_selector:
        print("❌ Sélecteur de liens introuvable par le LLM. Abandon de cette cible.")
        return []

    print(f"🎯 Sélecteur de liens PDP trouvé : {link_selector}")
    
    links = []
    base_url = urllib.parse.urljoin(target["url"], "/")
    try:
        elements = await page.query_selector_all(link_selector)
        if not elements:
            print(f"⚠️ Sélecteur Catalog '{link_selector}' n'a rien renvoyé. Tentative via [href*='/products/']...")
            elements = await page.query_selector_all("a[href*='/products/']")
            
        for element in elements:
            href = await element.get_attribute("href")
            if href:
                full_url = urllib.parse.urljoin(target["url"], href)
                # On évite les doublons et les paramètres de tracking
                full_url = full_url.split('?')[0]
                if "/products/" in full_url and full_url not in links:
                    links.append(full_url)
    except Exception as e:
        print(f"⚠️ Erreur lors de l'extraction Playwright des liens : {e}")
        
    print(f"✅ {len(links)} liens uniques de produits extraits.")
    return links

async def run_scout_analysis(page: Page, first_product_url: str) -> tuple[dict, Any]:
    """ÉTAPE 2 : The Scout Pattern pour extraire la sémantique de la page détail et l'API."""
    print(f"\n🕵️ ÉTAPE 2 : Agent Éclaireur activé (Scout Pattern) sur -> {first_product_url}")
    
    network_requests = []
    
    # Handler pour intercepter le trafic XHR/Fetch
    async def handle_response(response: Response):
        if response.request.resource_type in ["fetch", "xhr"]:
            try:
                content_type = response.headers.get("content-type", "")
                if "application/json" in content_type:
                    body = await response.text()
                    network_requests.append({
                        "url": response.request.url,
                        "method": response.request.method,
                        "status": response.status,
                        "preview_body": body[:500] # On limite la taille pour le LLM
                    })
            except Exception:
                pass

    # On attache l'intercepteur au navigateur pour les requêtes réseau
    page.on("response", handle_response)
    
    # Navigation sur le premier produit
    await page.goto(first_product_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(5000) # Donner du temps pour les requêtes réseau (reviews)
    
    html_content = await page.content()
    clean_html = clean_html_for_llm(html_content, max_chars=80000)
    
    # Phase 2A : Analyse des requêtes réseaux par le LLM (Avis du produit)
    reviews_api_url_template = None
    first_product_rating = None
    
    if network_requests:
        print("🔍 Scout analyse le trafic réseau pour l'API des reviews...")
        req_summary = json.dumps(network_requests, indent=2)
        
        prompt_network = f"""
        Voici un historique de requêtes réseau interceptées sur la page produit {first_product_url}.
        Trouve quelle requête récupère les avis ou la note (rating stars / score).
        Si tu trouves :
        1. "reviews_api_url_template": crée un template générique d'URL remplaçant l'identifiant du produit spécifique du lien par le mot strict "{{PRODUCT_ID}}". (Mets `null` si non trouvé).
        2. "current_product_stars": extrais et renvoie la note (étoiles) trouvée dans le corps (preview_body) de la réponse pour ce produit actuel. (Mets `null` si non trouvée).

        Réponds en JSON strict :
        {{
            "reviews_api_url_template": "https://api.../reviews?id={{PRODUCT_ID}}",
            "current_product_stars": 4.5
        }}
        Requêtes :
        {req_summary[:10000]}
        """
        net_analysis = await call_deepseek(prompt_network)
        reviews_api_url_template = net_analysis.get("reviews_api_url_template")
        first_product_rating = net_analysis.get("current_product_stars")
    
    if reviews_api_url_template:
        print(f"✨ API d'Avis trouvée ! URL type : {reviews_api_url_template}")
        print(f"⭐ Note du produit #1 (via réseau) : {first_product_rating}")
    else:
        print("⚠️ Aucune API d'avis exploitée dans les requêtes réseau interceptées.")
    
    # Phase 2B : Analyse HTML de la structure de la page produit PDP
    print("🔍 Scout analyse le HTML interne de la page pour cibler les données...")
    prompt_html = f"""
    Voici le HTML nettoyé d'une page produit PDP ({first_product_url}).
    Le site tourne probablement sous Shopify ou WooCommerce. 
    
    CONSIGNE CRITIQUE : Trouve des sélecteurs STABLES et GÉNÉRIQUES. Évite les IDs contenant des chiffres aléatoires (ex: #product-1234). 
    Privilégie les classes sémantiques (ex: .product-title, .price, .product-single__price).
    Regarde attentivement si un bloc <script type="application/ld+json"> existe, il contient souvent "name", "price" et "description".

    Extrais les sélecteurs CSS EXACTS pour :
    - "name_selector": le nom du produit (ex: "h1", ".product-title")
    - "price_selector": le prix (ex: ".price", "span[itemprop='price']", ".money")
    - "description_selector": le bloc de description (ex: ".product-description", "#description", ".rte")
    - "product_id_selector": l'élément contenant l'identifiant unique (ex: "input[name='id']", "meta[itemprop='sku']", "[data-product-id]")
    - "product_id_attribute": l'attribut à lire (ex: "value", "content", "data-product-id" ou "text")
    - "stars_selector": le sélecteur pour la note (étoiles/rating) (ex: ".jdgm-prev-badge__stars", ".rating-stars", ".avg-rating"). Ne réutilise PAS le sélecteur d'ID ici.
    - "stars_attribute": l'attribut contenant la note (ex: "data-score", "aria-label", "text").
    - "stock_selector": le sélecteur pour le stock (ex: ".inventory", ".stock-status", ".product-form__inventory").
    - "stock_attribute": l'attribut pour le stock (ex: "text", "data-stock").

    Réponds en JSON strict :
    {{
        "name_selector": "h1.product-title",
        "price_selector": "span.price-item--regular",
        "description_selector": ".product__description",
        "product_id_selector": "input[name='id']",
        "product_id_attribute": "value",
        "stars_selector": ".stars-container",
        "stars_attribute": "data-rating",
        "stock_selector": ".stock-status",
        "stock_attribute": "text"
    }}
    HTML:
    {clean_html}
    """
    scout_config = await call_deepseek(prompt_html)
    scout_config["reviews_api_url_template"] = reviews_api_url_template
    
    print(f"🎯 Sélecteurs trouvés par le Scout : {scout_config}")
    return scout_config, first_product_rating

async def extract_stars_via_llm(raw_json: str) -> Any:
    """Si le Harvester récupère un JSON complexe via l'API, il peut appeler une nano-tâche LLM."""
    prompt = f"""
    Voici du JSON provenant d'une API d'avis E-commerce. Trouve la note moyenne (rating stars) et le nombre d'avis (count).
    Renvoie juste un JSON (null si non trouvé) avec float et int.
    JSON cible: {raw_json[:2000]}
    
    Format:
    {{ "stars": 4.5, "reviews_count": 128 }}
    """
    res = await call_deepseek(prompt)
    if "stars" in res: return res
    return {"stars": res.get("rating"), "reviews_count": res.get("count")}

async def fast_track_extraction(target: dict, urls: List[str], scout_config: dict, first_rating: Any) -> List[Dict]:
    """ÉTAPE 3 : Le Moissonneur. Requêtes parallèles ou successives rapides avec httpx pour le bulk."""
    print("\n🚀 ÉTAPE 3 : Le Moissonneur (Fast-Track) est en marche !")
    scraped_data = []
    
    async with httpx.AsyncClient() as client:
        for idx, url in enumerate(urls):
            is_first = (idx == 0)
            print(f"⏳ Moissonnage produit {idx + 1}/{len(urls)}... {url}")
            
            try:
                response = await client.get(url, timeout=30.0)
                if response.status_code != 200:
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Fonction helper pour extraire rapidement via le sélecteur
                def extract(selector, attr=None):
                    if not selector: return None
                    el = soup.select_one(selector)
                    if not el: return None
                    return el.get(attr) if attr and attr != "text" else el.text.strip()
                
                # Attributs natifs de la PDP
                name = extract(scout_config.get("name_selector"))
                price = extract(scout_config.get("price_selector"))
                description = extract(scout_config.get("description_selector"))
                product_id = extract(scout_config.get("product_id_selector"), scout_config.get("product_id_attribute"))
                
                # FALLBACK 2 : Meta Tags (Si JSON-LD est absent ou incomplet)
                stock = extract(scout_config.get("stock_selector"), scout_config.get("stock_attribute"))
                
                if not name or not price or not product_id or not stock:
                    meta_data = extract_meta_tags(soup)
                    name = name or meta_data.get('name')
                    price = price or meta_data.get('price')
                    description = description or meta_data.get('description')
                    product_id = product_id or meta_data.get('product_id')
                    stock = stock or meta_data.get('stock')
                
                # FALLBACK 1 : JSON-LD (Si tjs vide)
                if not name or not price or not stock:
                    json_ld_data = extract_json_ld(soup)
                    name = name or json_ld_data.get('name')
                    price = price or json_ld_data.get('price')
                    description = description or json_ld_data.get('description')
                    product_id = product_id or json_ld_data.get('product_id')
                    stock = stock or json_ld_data.get('stock')
                
                # Gestion de la note et des avis
                stars = None
                reviews_count = None
                
                if is_first and first_rating is not None:
                    stars = first_rating
                
                # FALLBACK : JSON-LD (Souvent présent pour le SEO avec les notes)
                if not stars:
                    json_ld_data = extract_json_ld(soup)
                    stars = json_ld_data.get('stars')
                    if not reviews_count:
                        reviews_count = json_ld_data.get('reviews_count')

                # Tentative via API Review identifiée au début
                if not stars and scout_config.get("reviews_api_url_template") and product_id:
                    api_url = scout_config["reviews_api_url_template"].replace("{PRODUCT_ID}", str(product_id))
                    try:
                        api_resp = await client.get(api_url, timeout=10.0)
                        if api_resp.status_code == 200:
                            rating_info = await extract_stars_via_llm(api_resp.text)
                            stars = rating_info.get("stars")
                            reviews_count = rating_info.get("reviews_count")
                    except Exception as e:
                        print(f"⚠️ Échec du fetch d'avis pour l'id {product_id} : {e}")

                # FALLBACK : Tentative via sélecteur HTML (Si pas d'API ou API en échec)
                if not stars:
                    raw_stars = extract(scout_config.get("stars_selector"), scout_config.get("stars_attribute"))
                    if raw_stars:
                        # Validation : Si c'est un long nombre (ID), on ignore
                        str_stars = str(raw_stars).strip()
                        if len(str_stars) < 10: # Un rating type "4.5" ou "90%" est court
                            if "out of" in str_stars.lower(): 
                                try: stars = float(str_stars.split()[0])
                                except: pass
                            else:
                                # Nettoyage des caractères non numériques sauf point
                                import re
                                numeric_match = re.search(r"(\d+[\.,]?\d*)", str_stars)
                                if numeric_match:
                                    try: stars = float(numeric_match.group(1).replace(',', '.'))
                                    except: pass
                
                prod_item = {
                    "boutique": target.get("nom_boutique"),
                    "categorie": target.get("category"),
                    "nom": name,
                    "prix": price,
                    "description": description[:300] + "..." if description and len(description)>300 else description,
                    "note_etoiles": stars,
                    "nombre_avis": reviews_count,
                    "lien": url,
                    "product_id": product_id,
                    "stock": stock,
                    "date_extraction": datetime.datetime.now().isoformat()
                }
                scraped_data.append(prod_item)
                
            except Exception as e:
                print(f"❌ Erreur HTTP/Extraction sur la cible {url} : {e}")
                
    return scraped_data

async def smart_scrape(target: dict):
    """Pipeline asynchrone complet par cible."""
    async with async_playwright() as p:
        # On lance Chromium en headless complet
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # ÉTAPE 1
            urls = await extract_catalog_links(page, target)
            if not urls:
                print("Fin. Aucune URL PDP interceptée.")
                return

            # ÉTAPE 2
            first_url = urls[0]
            scout_config, first_rating = await run_scout_analysis(page, first_url)
            
            # Fermeture de playwright qui ne nous est plus utile
            await browser.close()
            
            # ÉTAPE 3
            results = await fast_track_extraction(target, urls, scout_config, first_rating)
            
            # ÉTAPE FINALE : SAUVEGARDE (Dossier data à la racine du projet)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # src/
            data_dir = os.path.join(os.path.dirname(project_root), "data")
            os.makedirs(data_dir, exist_ok=True)
            
            safe_name = str(target.get('nom_boutique', 'Cible')).replace(' ', '_')
            cat = target.get('category', 'divers')
            filename = os.path.join(data_dir, f"scraped_{safe_name}_{cat}.json")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)
                
            print(f"\n🎉 EXCELLENT ! {len(results)} produits moissonnés avec description et étoiles !")
            print(f"💾 Sauvegardés dans le json : {filename}")

        except Exception as e:
            print(f"\n❌ Erreur fatale sur la cible {target['nom_boutique']} : {e}")
            if browser.is_connected():
                await browser.close()

async def main():
    parser = argparse.ArgumentParser(description="Agent Scraper - Intelligence Produit")
    parser.add_argument("--target", type=str, help="Nom de la boutique à scraper (ex: Blackview)")
    parser.add_argument("--list", action="store_true", help="Liste les boutiques disponibles")
    
    args = parser.parse_args()
    targets = get_scraping_targets()
    
    if args.list:
        print("\n🎯 Boutiques disponibles pour le scraping :")
        for t in targets:
            print(f"- {t['nom_boutique']} (Catégorie: {t['category']})")
        return

    if args.target:
        # Filtrage par nom de boutique (insensible à la casse)
        selected = [t for t in targets if t['nom_boutique'].lower() == args.target.lower()]
        if not selected:
            print(f"❌ Erreur : La boutique '{args.target}' n'existe pas dans generator_agent.py.")
            print("Utilisez --list pour voir les options.")
            return
        
        print(f"🎯 Lancement du Scraping ciblé -> {selected[0]['nom_boutique']}")
        await smart_scrape(selected[0])
    else:
        print(f"🎯 Mode Séquentiel : Total de {len(targets)} cibles à scraper.")
        for target in targets:
            await smart_scrape(target)

if __name__ == "__main__":
    asyncio.run(main())