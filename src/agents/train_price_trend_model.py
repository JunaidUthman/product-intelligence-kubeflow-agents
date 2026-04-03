import os
import logging
import json
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple
import joblib
from xgboost import XGBClassifier
from sklearn.metrics import classification_report
from dotenv import load_dotenv
from huggingface_hub import HfApi

# Import de la configuration DB
from database_utils import get_db_config, get_connection

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def extract_data_from_db() -> pd.DataFrame:
    """Extrait les données historiques de MySQL."""
    logger.info("📡 Extraction des données depuis MySQL...")
    try:
        conn = get_connection()
        query = """
        SELECT 
            ps.product_id, 
            ps.prix_usd, 
            ss.date_session
        FROM product_scores ps
        JOIN scraping_sessions ss ON ps.session_id = ss.id
        ORDER BY ps.product_id, ss.date_session ASC
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall() # DictCursor returns a list of dicts
            
        conn.close()
        
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Conversion explicite des types
        df['prix_usd'] = pd.to_numeric(df['prix_usd'], errors='coerce')
        df['date_session'] = pd.to_datetime(df['date_session'])
        
        # Nettoyage des lignes corrompues
        initial_len = len(df)
        df = df.dropna(subset=['prix_usd', 'date_session'])
        if len(df) < initial_len:
            logger.warning(f"⚠️ {initial_len - len(df)} lignes incorrectes ont été ignorées.")
            
        logger.info(f"✅ {len(df)} lignes valides extraites.")
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'extraction : {e}")
        raise

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Crée les variables temporelles (Lags, Volatilité) par produit."""
    logger.info("🛠️ Feature Engineering en cours...")
    
    # S'assurer que les données sont bien triées par date par produit
    df = df.sort_values(['product_id', 'date_session']).copy()
    
    # Groupement par produit pour les calculs temporels
    grouped = df.groupby('product_id')
    
    # 1. Lags de prix
    df['prix_lag_1'] = grouped['prix_usd'].shift(1)
    df['prix_lag_3'] = grouped['prix_usd'].shift(3)
    df['prix_lag_7'] = grouped['prix_usd'].shift(7)
    
    # 2. Volatilité (écart-type sur 7 jours)
    df['volatilite_7j'] = grouped['prix_usd'].rolling(window=7).std().values
    
    # 3. Gestion des NaNs par produit
    cols_to_fill = ['prix_lag_1', 'prix_lag_3', 'prix_lag_7', 'volatilite_7j']
    df[cols_to_fill] = df.groupby('product_id')[cols_to_fill].ffill().bfill()
    
    logger.info("✅ Variables temporelles créées.")
    return df

def create_target(df: pd.DataFrame, horizon_days: int = 7) -> pd.DataFrame:
    """Crée la variable cible multiclasse via time-shifting futur."""
    logger.info(f"🎯 Création de la target (horizon {horizon_days} jours)...")
    
    # On regarde le prix futur à J+7
    df['future_price'] = df.groupby('product_id')['prix_usd'].shift(-horizon_days)
    
    # Calcul du % de variation
    df['variation_pct'] = (df['future_price'] - df['prix_usd']) / df['prix_usd']
    
    def label_trend(val):
        if pd.isna(val): return np.nan
        if val < -0.01: return 0 # BAISSE
        if val > 0.01: return 2  # HAUSSE
        return 1 # STABLE
    
    df['target'] = df['variation_pct'].apply(label_trend)
    df_clean = df.dropna(subset=['target']).copy()
    
    logger.info(f"✅ Target créée. Taille finale : {len(df_clean)} lignes.")
    return df_clean

def train_and_export_model(df: pd.DataFrame):
    """Effectue un split temporel, entraîne XGBoost et sauvegarde."""
    logger.info("🚀 Début de l'entraînement...")
    
    features = ['prix_usd', 'prix_lag_1', 'prix_lag_3', 'prix_lag_7', 'volatilite_7j']
    X = df[features]
    y = df['target'].astype(int)
    
    # Temporal Split (80/20)
    dates_uniques = sorted(df['date_session'].unique())
    if len(dates_uniques) < 2:
        logger.warning("⚠️ Pas assez de dates différentes pour un split temporel strict.")
        split_date = dates_uniques[0] if dates_uniques else None
    else:
        split_idx = int(len(dates_uniques) * 0.8)
        split_date = dates_uniques[split_idx]
    
    if split_date:
        train_mask = df['date_session'] < split_date
        X_train, X_test = X[train_mask], X[~train_mask]
        y_train, y_test = y[train_mask], y[~train_mask]
    else:
        X_train, X_test, y_train, y_test = X, X, y, y # Dev mode fallback
        
    if len(X_train) == 0:
        logger.error("❌ Le set d'entraînement est vide.")
        return

    logger.info(f"📅 Train set: {len(X_train)} / Test set: {len(X_test)}")
    
    # XGBClassifier
    model = XGBClassifier(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        objective='multi:softprob',
        num_class=3,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred, target_names=['BAISSE', 'STABLE', 'HAUSSE'], zero_division=0)
    logger.info(f"📊 Rapport de classification :\n\n{report}")
    
    # 5. Sauvegarde dans le dossier models/ à la racine du projet
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    models_dir = os.path.join(project_root, 'models')
    os.makedirs(models_dir, exist_ok=True)
    export_path = os.path.join(models_dir, 'xgboost_trend_model.pkl')
    joblib.dump(model, export_path)
    logger.info(f"💾 Modèle sauvegardé dans '{export_path}'")
    return export_path

def upload_to_hf(file_path: str):
    """Téléverse le modèle sur Hugging Face Hub de manière automatique."""
    logger.info("☁️ Préparation du téléversement vers Hugging Face Hub...")
    try:
        token = os.environ.get("HF_TOKEN")
        repo_id = os.environ.get("HF_REPO_ID")
        
        if not token or not repo_id:
            logger.warning("⚠️ HF_TOKEN ou HF_REPO_ID manquant dans le .env. Téléversement annulé.")
            return

        api = HfApi()
        api.upload_file(
            path_or_fileobj=file_path,
            path_in_repo=os.path.basename(file_path),
            repo_id=repo_id,
            token=token,
            commit_message=f"Auto-update: price trend model {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        logger.info(f"🚀 Modèle téléversé avec succès sur : {repo_id}")
    except Exception as e:
        logger.error(f"❌ Échec du téléversement Hugging Face : {e}")

if __name__ == "__main__":
    try:
        data = extract_data_from_db()
        if not data.empty:
            data = feature_engineering(data)
            data = create_target(data)
            if not data.empty:
                export_path = train_and_export_model(data)
                if export_path:
                    upload_to_hf(export_path)
            else:
                logger.warning("⚠️ Dataset vide après création de la target.")
        else:
            logger.error("❌ Aucune donnée extraite.")
    except Exception as e:
        logger.error(f"💥 Échec critique : {e}")
