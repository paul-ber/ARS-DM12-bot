import pandas as pd
import numpy as np
from tqdm import tqdm
from baac_loader import BAACLoader
from enrichers import OverpassEnricher
from elk_pusher import ElasticPusher
from enrichment_processor import EnrichmentProcessor, get_accidents_to_enrich, update_elk_with_enrichment
import os
import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

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

    parser.add_argument("--data-dir", type=str, default="data/raw")
    parser.add_argument("--cache-dir", type=str, default="data/cache")
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--force-reload", action="store_true")
    parser.add_argument("--n-jobs", type=int, default=10)

    parser.add_argument("--skip-overpass", action="store_true")
    parser.add_argument("--overpass-url", type=str, default="http://localhost:12345/api/interpreter")
    parser.add_argument("--overpass-radius", type=int, default=1000)
    parser.add_argument("--overpass-min-year", type=int, default=None)
    parser.add_argument("--overpass-workers", type=int, default=10)

    parser.add_argument("--enrich-only", action="store_true")

    parser.add_argument("--send-elk", action="store_true")
    parser.add_argument("--elk-host", type=str, default="localhost")
    parser.add_argument("--elk-port", type=int, default=9200)
    parser.add_argument("--elk-user", type=str, default=os.getenv("ELK_USER"))
    parser.add_argument("--elk-password", type=str, default=os.getenv("ELK_PASSWORD"))
    parser.add_argument("--batch-size", type=int, default=500)

    parser.add_argument("--verbose", action="store_true")

    return parser.parse_args()


def mode_enrich_only(args):
    """Mode enrichissement : met à jour les accidents avec Overpass"""
    logger.info("MODE ENRICHISSEMENT")

    if not args.send_elk:
        logger.error("--enrich-only nécessite --send-elk")
        sys.exit(1)

    pusher = ElasticPusher(
        host=args.elk_host,
        port=args.elk_port,
        user=args.elk_user,
        password=args.elk_password
    )

    accidents_to_enrich = get_accidents_to_enrich(pusher, min_year=args.overpass_min_year)

    if not accidents_to_enrich:
        logger.info("Tous les accidents sont déjà enrichis !")
        return

    for acc in accidents_to_enrich:
        acc['radius'] = args.overpass_radius

    overpass_enricher = OverpassEnricher(base_url=args.overpass_url)
    processor = EnrichmentProcessor(overpass_enricher)

    enriched_data = processor.enrich_batch(accidents_to_enrich, n_jobs=args.overpass_workers)

    update_elk_with_enrichment(pusher, enriched_data, batch_size=args.batch_size)


def mode_import(args):
    """Mode import : charge les données BAAC et les envoie vers ELK"""

    # 1. Chargement BAAC
    logger.info("\n[1/4] Chargement des données BAAC...")
    loader = BAACLoader(data_dir=args.data_dir, cache_dir=args.cache_dir)
    data = loader.load_all_years(n_jobs=args.n_jobs, force_reload=args.force_reload)

    df_accidents = data['accidents']
    df_vehicules = data['vehicules']
    df_usagers = data['usagers']

    logger.info(f"{len(df_accidents):,} accidents, {len(df_vehicules):,} véhicules, {len(df_usagers):,} usagers")

    # 2. Échantillonnage
    if args.sample_size:
        logger.info(f"\Échantillonnage de {args.sample_size} accidents")
        sample_ids = df_accidents['num_acc'].sample(min(args.sample_size, len(df_accidents)))
        df_accidents = df_accidents[df_accidents['num_acc'].isin(sample_ids)]
        df_vehicules = df_vehicules[df_vehicules['num_acc'].isin(sample_ids)]
        df_usagers = df_usagers[df_usagers['num_acc'].isin(sample_ids)]
        logger.info(f"{len(df_accidents)} accidents sélectionnés")

    # 3. Connexion ELK
    pusher = None
    if args.send_elk:
        logger.info(f"\n[2/4] Connexion à Elasticsearch")
        pusher = ElasticPusher(
            host=args.elk_host,
            port=args.elk_port,
            user=args.elk_user,
            password=args.elk_password
        )
        pusher.create_accidents_index()
        pusher.create_vehicules_index()
        pusher.create_usagers_index()
    else:
        logger.info("\n[2/4] Envoi Elasticsearch désactivé")
        return

    # 4. Envoi ACCIDENTS
    logger.info(f"\n[3/4] Envoi des accidents...")
    batch_acc = []

    for _, row in tqdm(df_accidents.iterrows(), total=len(df_accidents), desc="Accidents"):
        doc = convert_to_json_serializable(row.to_dict())

        # Ajouter geo_point
        if doc.get('lat') and doc.get('long'):
            doc['coords'] = {"lat": doc['lat'], "lon": doc['long']}

        batch_acc.append(doc)

        if len(batch_acc) >= args.batch_size:
            pusher.push_documents(batch_acc, "accidents-routiers")
            batch_acc = []

    if batch_acc:
        pusher.push_documents(batch_acc, "accidents-routiers")

    # 5. Envoi VEHICULES
    logger.info(f"\n[4/4] Envoi des véhicules et usagers...")
    batch_veh = []

    for _, row in tqdm(df_vehicules.iterrows(), total=len(df_vehicules), desc="Véhicules"):
        doc = convert_to_json_serializable(row.to_dict())
        batch_veh.append(doc)

        if len(batch_veh) >= args.batch_size:
            pusher.push_documents(batch_veh, "accidents-vehicules")
            batch_veh = []

    if batch_veh:
        pusher.push_documents(batch_veh, "accidents-vehicules")

    # 6. Envoi USAGERS
    batch_usr = []

    for _, row in tqdm(df_usagers.iterrows(), total=len(df_usagers), desc="Usagers"):
        doc = convert_to_json_serializable(row.to_dict())

        # Calculer l'âge
        if doc.get('an_nais') and doc.get('an_nais') > 1900:
            doc['age'] = 2026 - doc['an_nais']

        batch_usr.append(doc)

        if len(batch_usr) >= args.batch_size:
            pusher.push_documents(batch_usr, "accidents-usagers")
            batch_usr = []

    if batch_usr:
        pusher.push_documents(batch_usr, "accidents-usagers")

    # Stats finales
    logger.info("\n" + "="*60)
    logger.info("IMPORT TERMINÉ")
    logger.info("="*60)
    logger.info(f"Accidents importés : {len(df_accidents):,}")
    logger.info(f"Véhicules importés : {len(df_vehicules):,}")
    logger.info(f"Usagers importés   : {len(df_usagers):,}")
    logger.info("="*60)

    if not args.skip_overpass:
        logger.info("\nPour enrichir avec Overpass :")
        logger.info("python src/main.py --enrich-only --send-elk --overpass-min-year 2022 --overpass-workers 20")


def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger("DM12").setLevel(logging.DEBUG)

    logger.info("="*60)
    logger.info("🚀 PIPELINE BAAC - 3 INDEX")
    logger.info("="*60)

    try:
        if args.enrich_only:
            mode_enrich_only(args)
        else:
            mode_import(args)
    except KeyboardInterrupt:
        logger.error("\nInterruption")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"\nERREUR : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
