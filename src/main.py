from baac_loader import BAACLoader
from elk_pusher import ElasticPusher
from enrichers import OverpassEnricher

import os
import sys
import logging
import argparse
from utils import *
from tqdm import tqdm
from dotenv import load_dotenv
from enrichment_processor import EnrichmentProcessor, get_accidents_to_enrich, update_elk_with_enrichment

load_dotenv()


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
        acc["radius"] = args.overpass_radius

    overpass_enricher = OverpassEnricher(base_url=args.overpass_url)
    processor = EnrichmentProcessor(overpass_enricher)

    enriched_data = processor.enrich_batch(accidents_to_enrich, n_jobs=args.overpass_workers)

    update_elk_with_enrichment(pusher, enriched_data, batch_size=args.batch_size)

def mode_import(args):
    """Mode import : charge les données BAAC et les envoie vers ELK"""

    # [1/6] CHARGEMENT BAAC
    logger.info("=" * 60)
    logger.info("[1/6] Chargement des données BAAC...")
    loader = BAACLoader(data_dir=args.data_dir, cache_dir=args.cache_dir)
    data = loader.load_all_years(n_jobs=args.n_jobs, force_reload=args.force_reload)

    df_accidents = data["accidents"]
    df_lieux = data["lieux"]
    df_vehicules = data["vehicules"]
    df_usagers = data["usagers"]

    logger.info(f"{len(df_accidents)} accidents, {len(df_lieux)} lieux, "
               f"{len(df_vehicules)} véhicules, {len(df_usagers)} usagers")

    # [2/6] ÉCHANTILLONNAGE
    if args.sample_size:
        logger.info(f"[2/6] Échantillonnage de {args.sample_size} accidents")
        sample_ids = df_accidents["num_acc"].sample(min(args.sample_size, len(df_accidents)))
        df_accidents = df_accidents[df_accidents["num_acc"].isin(sample_ids)]
        df_lieux = df_lieux[df_lieux["num_acc"].isin(sample_ids)]
        df_vehicules = df_vehicules[df_vehicules["num_acc"].isin(sample_ids)]
        df_usagers = df_usagers[df_usagers["num_acc"].isin(sample_ids)]
        logger.info(f"{len(df_accidents)} accidents sélectionnés")

    # [3/6] CONNEXION ELK
    pusher = None
    if args.send_elk:
        logger.info(f"[3/6] Connexion à Elasticsearch")
        pusher = ElasticPusher(
            host=args.elk_host,
            port=args.elk_port,
            user=args.elk_user,
            password=args.elk_password
        )

        pusher.create_accidents_index()
        pusher.create_lieux_index()
        pusher.create_vehicules_index()
        pusher.create_usagers_index()
    else:
        logger.info("[3/6] Envoi Elasticsearch désactivé")
        return

    # [4/6] ENVOI ACCIDENTS
    logger.info(f"[4/6] Envoi des accidents (caractéristiques)...")
    batch_acc = []
    for _, row in tqdm(df_accidents.iterrows(), total=len(df_accidents), desc="Accidents"):
        doc = convert_to_json_serializable(row.to_dict())

        if doc.get("lat") and doc.get("long"):
            doc["coords"] = {"lat": doc["lat"], "lon": doc["long"]}

        batch_acc.append(doc)

        if len(batch_acc) >= args.batch_size:
            pusher.push_documents(batch_acc, "accidents-caracteristiques")
            batch_acc = []

    if batch_acc:
        pusher.push_documents(batch_acc, "accidents-caracteristiques")

    # [5/6] ENVOI LIEUX
    logger.info(f"[5/6] Envoi des lieux...")
    batch_lieux = []
    for _, row in tqdm(df_lieux.iterrows(), total=len(df_lieux), desc="Lieux"):
        doc = convert_to_json_serializable(row.to_dict())
        batch_lieux.append(doc)

        if len(batch_lieux) >= args.batch_size:
            pusher.push_documents(batch_lieux, "accidents-lieux")
            batch_lieux = []

    if batch_lieux:
        pusher.push_documents(batch_lieux, "accidents-lieux")

    # [6/6] ENVOI VÉHICULES ET USAGERS
    logger.info(f"[6/6] Envoi des véhicules et usagers...")

    # Véhicules
    batch_veh = []
    for _, row in tqdm(df_vehicules.iterrows(), total=len(df_vehicules), desc="Véhicules"):
        doc = convert_to_json_serializable(row.to_dict())
        batch_veh.append(doc)

        if len(batch_veh) >= args.batch_size:
            pusher.push_documents(batch_veh, "accidents-vehicules")
            batch_veh = []

    if batch_veh:
        pusher.push_documents(batch_veh, "accidents-vehicules")

    # Usagers
    batch_usr = []
    for _, row in tqdm(df_usagers.iterrows(), total=len(df_usagers), desc="Usagers"):
        doc = convert_to_json_serializable(row.to_dict())

        if doc.get("annais") and doc.get("annais") > 1900:
            doc["age"] = 2026 - doc["annais"]

        batch_usr.append(doc)

        if len(batch_usr) >= args.batch_size:
            pusher.push_documents(batch_usr, "accidents-usagers")
            batch_usr = []

    if batch_usr:
        pusher.push_documents(batch_usr, "accidents-usagers")

    # STATS FINALES
    logger.info("=" * 60)
    logger.info("IMPORT TERMINÉ")
    logger.info("=" * 60)
    logger.info(f"Accidents importés: {len(df_accidents)}")
    logger.info(f"Lieux importés: {len(df_lieux)}")
    logger.info(f"Véhicules importés: {len(df_vehicules)}")
    logger.info(f"Usagers importés: {len(df_usagers)}")
    logger.info("=" * 60)

    if not args.skip_overpass:
        logger.info("Pour enrichir avec Overpass:")
        logger.info("python src/main.py --enrich-only --send-elk --overpass-min-year 2022 --overpass-workers 20")

def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger("DM12").setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("PIPELINE BAAC")
    logger.info("=" * 60)

    try:
        if args.enrich_only:
            mode_enrich_only(args)
        else:
            mode_import(args)

    except KeyboardInterrupt:
        logger.error("Interruption")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"{e}")
        sys.exit(1)

if __name__ == "__main__":
    main()