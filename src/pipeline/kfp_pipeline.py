import os
import json
from kfp import dsl, kubernetes
from kfp.compiler import Compiler

# ==============================================================================
# CONFIGURATION KFP
# ==============================================================================
PVC_NAME = "product-intel-pvc"
SECRET_NAME = "product-intel-secrets"
IMAGE_NAME = "product-intel-agents:v1"
DATA_DIR = "/app/data"

# ==============================================================================
# COMPOSANTS (Wrappers pour les agents existants)
# ==============================================================================

@dsl.container_component
def generator_task(output_list: dsl.OutputPath(list)):
    """Charge la liste des cibles e-commerce."""
    return dsl.ContainerSpec(
        image=IMAGE_NAME,
        command=["python3"],
        args=["src/agents/generator_agent.py", "--json", output_list]
    )

@dsl.container_component
def scraper_task(boutique: str, url: str, category: str, platform: str):
    """Lance un scraper individuel pour une cible donnée."""
    return dsl.ContainerSpec(
        image=IMAGE_NAME,
        command=["python3"],
        args=[
            "src/agents/scraper_agent.py",
            "--boutique", boutique,
            "--url", url,
            "--category", category,
            "--platform", platform
        ]
    )

@dsl.container_component
def processor_task():
    """Agrège, nettoie et insère les données dans MySQL."""
    return dsl.ContainerSpec(
        image=IMAGE_NAME,
        command=["python3"],
        args=["src/agents/processor_agent.py"]
    )

@dsl.container_component
def ranking_task():
    """Calcule les scores k-top dans la base de données."""
    return dsl.ContainerSpec(
        image=IMAGE_NAME,
        command=["python3"],
        args=["src/agents/ranking_agent.py"]
    )

@dsl.container_component
def training_task():
    """Entraîne le modèle XGBoost et exporte vers Hugging Face."""
    return dsl.ContainerSpec(
        image=IMAGE_NAME,
        command=["python3"],
        args=["src/agents/train_price_trend_model.py"]
    )

@dsl.pipeline(name="scraping-group", description="Exécute les scrapers en parallèle")
def scraping_group(targets: list):
    with dsl.ParallelFor(targets) as target:
        scrape = scraper_task(
            boutique=target.nom_boutique,
            url=target.url,
            category=target.category,
            platform=target.platform
        )
        scrape.set_display_name("Parallel Scraper")
        kubernetes.mount_pvc(scrape, pvc_name=PVC_NAME, mount_path=DATA_DIR)
        scrape.set_env_variable("DATA_DIR", DATA_DIR)
        for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE", "DEEPSEEK_API_KEY"]:
            kubernetes.use_secret_as_env(scrape, secret_name=SECRET_NAME, secret_key_to_env={key: key})

@dsl.pipeline(
    name="product-intelligence-full-pipeline",
    description="Pipeline A to Z : Generation -> Parallel Scraping -> Processing -> Ranking -> Training"
)
def product_intel_pipeline():
    # 1. Génération des cibles
    gen = generator_task()
    
    # 2. Scraping en Parallèle (via sous-pipeline)
    scrapes = scraping_group(targets=gen.outputs['output_list'])

    # 3. Processeur
    proc = processor_task().after(scrapes)
    kubernetes.mount_pvc(proc, pvc_name=PVC_NAME, mount_path=DATA_DIR)
    proc.set_env_variable("DATA_DIR", DATA_DIR)
    for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]:
        kubernetes.use_secret_as_env(proc, secret_name=SECRET_NAME, secret_key_to_env={key: key})

    # 4. Ranking
    rank = ranking_task().after(proc)
    for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]:
        kubernetes.use_secret_as_env(rank, secret_name=SECRET_NAME, secret_key_to_env={key: key})

    # 5. Training
    train = training_task().after(rank)
    for key in ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE", "HF_TOKEN", "HF_REPO_ID"]:
        kubernetes.use_secret_as_env(train, secret_name=SECRET_NAME, secret_key_to_env={key: key})

# ==============================================================================
# COMPILATION
# ==============================================================================
if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=product_intel_pipeline,
        package_path="product_intel_pipeline.yaml"
    )
    print("✅ Pipeline compilé avec succès : product_intel_pipeline.yaml")
