import pandas as pd
import numpy as np
from tqdm import tqdm
from baac_loader import BAACLoader
from enrichers import OverpassEnricher
from elk_pusher import ElasticPusher
from enrichment_processor import EnrichmentProcessor, get_accidents_to_enrich, update_elk_with_enrichment
import json
import os
import argparse
import sys
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# --- LOGGING GLOBAL ---
import logging

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8")
    ],
)
logger = logging.getLogger("DM12")


def convert_to_json_serializable(obj):
    """Convertit récursivement les types pandas/numpy en types JSON natifs."""
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat() if pd.notna(obj) else None
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return None if np.isnan(obj) else float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif pd.isna(obj):
        return None
    else:
        return obj


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline d'enrichissement des données BAAC",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Données
    parser.add_argument("--data-dir", type=str, default="data/raw",
                       help="Répertoire contenant les données BAAC brutes")
    parser.add_argument("--cache-dir", type=str, default="data/cache",
                       help="Répertoire de cache pour joblib")
    parser.add_argument("--sample-size", type=int, default=None,
                       help="Nombre d'accidents à traiter (None = tous)")
    parser.add_argument("--force-reload", action="store_true",
                       help="Force le rechargement des données BAAC")
    parser.add_argument("--n-jobs", type=int, default=10,
                       help="Nombre de workers pour le chargement BAAC")
    
    # Overpass
    parser.add_argument("--skip-overpass", action="store_true",
                       help="Désactive l'enrichissement Overpass")
    parser.add_argument("--overpass-url", type=str, 
                       default="http://localhost:12345/api/interpreter",
                       help="URL de l'API Overpass")
    parser.add_argument("--overpass-radius", type=int, default=1000,
                       help="Rayon de recherche Overpass en mètres")
    parser.add_argument("--overpass-min-year", type=int, default=None,
                       help="Année minimale pour enrichissement Overpass")
    parser.add_argument("--overpass-workers", type=int, default=10,
                       help="Nombre de workers parallèles Overpass")
    
    # Mode enrichissement seul
    parser.add_argument("--enrich-only", action="store_true", 
                       help="Mode enrichissement : met à jour les documents ELK existants")
    
    # Export
    parser.add_argument("--save-json", action="store_true",
                       help="Sauvegarde les résultats en JSON")
    parser.add_argument("--json-output", type=str, 
                       default="data/output/accidents_enriched.json",
                       help="Chemin du fichier JSON de sortie")
    
    # Elasticsearch
    parser.add_argument("--send-elk", action="store_true",
                       help="Envoie les données vers Elasticsearch")
    parser.add_argument("--elk-host", type=str, default="localhost",
                       help="Hôte Elasticsearch")
    parser.add_argument("--elk-port", type=int, default=9200,
                       help="Port Elasticsearch")
    parser.add_argument("--elk-index", type=str, default="accidents-routiers",
                       help="Nom de l'index Elasticsearch")
    parser.add_argument("--elk-user", type=str, default=os.getenv("ELK_USER"),
                       help="Username Elasticsearch")
    parser.add_argument("--elk-password", type=str, default=os.getenv("ELK_PASSWORD"),
                       help="Password Elasticsearch")
    parser.add_argument("--batch-size", type=int, default=500,
                       help="Taille des batchs pour l'envoi vers ELK")
    
    # Debug
    parser.add_argument("--verbose", action="store_true",
                       help="Mode debug (logs détaillés)")
    
    return parser.parse_args()


def mode_enrich_only(args):
    """Mode enrichissement : met à jour les documents ELK existants"""
    logger.info("MODE ENRICHISSEMENT : Mise à jour des documents ELK")
    
    if not args.send_elk:
        logger.error("--enrich-only nécessite --send-elk")
        sys.exit(1)
    
    # Connexion ELK
    pusher = ElasticPusher(
        host=args.elk_host, 
        port=args.elk_port, 
        index_name=args.elk_index,
        user=args.elk_user, 
        password=args.elk_password
    )
    
    # Récupérer les accidents à enrichir depuis ELK
    accidents_to_enrich = get_accidents_to_enrich(pusher, min_year=args.overpass_min_year)
    
    if not accidents_to_enrich:
        logger.info("Tous les accidents sont déjà enrichis !")
        return
    
    # Ajouter le radius
    for acc in accidents_to_enrich:
        acc['radius'] = args.overpass_radius
    
    # Enrichissement parallèle
    overpass_enricher = OverpassEnricher(base_url=args.overpass_url)
    processor = EnrichmentProcessor(overpass_enricher)
    
    enriched_data = processor.enrich_batch(accidents_to_enrich, n_jobs=args.overpass_workers)
    
    # Mise à jour ELK
    update_elk_with_enrichment(pusher, enriched_data, batch_size=args.batch_size)


def mode_import(args):
    """Mode import : charge les données BAAC et les envoie vers ELK"""
    
    # 1. Chargement BAAC
    logger.info("\n[1/3] Chargement des données BAAC...")
    loader = BAACLoader(data_dir=args.data_dir, cache_dir=args.cache_dir)
    data = loader.load_all_years(n_jobs=args.n_jobs, force_reload=args.force_reload)
    
    df_accidents = data['accidents']
    df_vehicules = data['vehicules']
    df_usagers = data['usagers']
    
    logger.info(f"{len(df_accidents):,} accidents chargés")
    
    # 2. Échantillonnage
    if args.sample_size:
        logger.info(f"\n🧪 Échantillonnage de {args.sample_size} accidents")
        sample_ids = df_accidents['num_acc'].sample(min(args.sample_size, len(df_accidents)))
        df_accidents = df_accidents[df_accidents['num_acc'].isin(sample_ids)]
        df_vehicules = df_vehicules[df_vehicules['num_acc'].isin(sample_ids)]
        df_usagers = df_usagers[df_usagers['num_acc'].isin(sample_ids)]
        logger.info(f"✅ {len(df_accidents)} accidents sélectionnés")
    
    # 3. Connexion ELK
    pusher = None
    if args.send_elk:
        logger.info(f"\n[2/3] Connexion à Elasticsearch {args.elk_host}:{args.elk_port}")
        try:
            pusher = ElasticPusher(
                host=args.elk_host, 
                port=args.elk_port, 
                index_name=args.elk_index,
                user=args.elk_user, 
                password=args.elk_password
            )
            pusher.create_index_if_not_exists()
            logger.info(f"Index '{args.elk_index}' prêt")
        except Exception as e:
            logger.error(f"Impossible de se connecter à ELK : {e}")
            pusher = None
    else:
        logger.info("\n[2/3] Envoi Elasticsearch désactivé")
    
    # 4. Construction des documents
    logger.info(f"\n[3/3] Construction et envoi des documents...")
    
    batch = []
    all_results = [] if args.save_json else None
    
    vehicules_by_acc = df_vehicules.groupby('num_acc')
    usagers_by_acc = df_usagers.groupby('num_acc')
    
    for _, row_acc in tqdm(df_accidents.iterrows(), total=len(df_accidents), desc="Import"):
        num_acc = row_acc['num_acc']
        
        # Construction document
        enriched_doc = {
            "id_accident": str(num_acc),
            "timestamp": row_acc.get('timestamp').isoformat() if pd.notna(row_acc.get('timestamp')) else None,
            "caracteristiques": convert_to_json_serializable(row_acc.to_dict()),
            "vehicules": convert_to_json_serializable(
                [v.to_dict() for _, v in vehicules_by_acc.get_group(num_acc).iterrows()]
                if num_acc in vehicules_by_acc.groups else []
            ),
            "usagers": convert_to_json_serializable(
                [u.to_dict() for _, u in usagers_by_acc.get_group(num_acc).iterrows()]
                if num_acc in usagers_by_acc.groups else []
            ),
            "infrastructure_env": None
        }
        
        batch.append(enriched_doc)
        
        if args.save_json:
            all_results.append(enriched_doc)
        
        # Envoi batch
        if len(batch) >= args.batch_size and pusher:
            try:
                pusher.push_documents(batch)
            except Exception as e:
                logger.error(f"Erreur push batch : {e}")
            batch = []
    
    # Dernier batch
    if batch and pusher:
        logger.info(f"\nEnvoi du dernier batch ({len(batch)} documents)...")
        pusher.push_documents(batch)
    
    # Sauvegarde JSON
    if args.save_json and all_results:
        logger.info(f"\n💾 Sauvegarde JSON dans {args.json_output}...")
        os.makedirs(os.path.dirname(args.json_output), exist_ok=True)
        with open(args.json_output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ {len(all_results)} accidents sauvegardés")
    
    # Stats finales
    logger.info("\n" + "="*60)
    logger.info("IMPORT TERMINÉ")
    logger.info("="*60)
    logger.info(f"Total accidents    : {len(df_accidents):,}")
    logger.info(f"Envoyé vers ELK    : {'OUI' if pusher else 'NON'}")
    logger.info(f"JSON sauvegardé    : {'OUI' if args.save_json else 'NON'}")
    logger.info("="*60)
    
    if pusher and not args.skip_overpass:
        logger.info("\n💡 Pour enrichir avec Overpass :")
        logger.info(f"python src/main.py --enrich-only --send-elk \\")
        logger.info(f"  --overpass-min-year 2022 \\")
        logger.info(f"  --overpass-workers {args.overpass_workers}")


def main():
    args = parse_args()
    
    if args.verbose:
        logging.getLogger("DM12").setLevel(logging.DEBUG)
    
    logger.info("="*60)
    logger.info("PIPELINE BAAC")
    logger.info("="*60)
    
    try:
        if args.enrich_only:
            mode_enrich_only(args)
        else:
            mode_import(args)
    except KeyboardInterrupt:
        logger.error("\nInterruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"\nERREUR : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()