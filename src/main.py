import pandas as pd
from tqdm import tqdm
from baac_loader import BAACLoader
from enrichers import OverpassEnricher
from elk_pusher import ElasticPusher
import json
import os
import argparse
import sys
import numpy as np
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

def parse_args():
    """Parse les arguments en ligne de commande"""
    parser = argparse.ArgumentParser(
        description="Pipeline d'enrichissement des données BAAC avec Overpass et envoi vers Elasticsearch",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Données source
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/raw",
        help="Répertoire contenant les données BAAC brutes (par année)"
    )

    parser.add_argument(
        "--cache-dir",
        type=str,
        default="data/cache",
        help="Répertoire de cache pour joblib et données chargées"
    )

    # Échantillonnage
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Nombre d'accidents à traiter (None = tous)"
    )

    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Force le rechargement des données BAAC même si le cache existe"
    )

    # Overpass
    parser.add_argument(
        "--overpass-url",
        type=str,
        default="http://localhost:12345/api/interpreter",
        help="URL de l'API Overpass (locale ou distante)"
    )

    parser.add_argument(
        "--overpass-radius",
        type=int,
        default=1000,
        help="Rayon de recherche Overpass en mètres"
    )

    parser.add_argument(
        "--overpass-min-year",
        type=int,
        default=None,
        help="Année minimale pour enrichissement Overpass (ex: 2022 pour 2022-2025)"
    )

    parser.add_argument(
        "--skip-overpass",
        action="store_true",
        help="Désactive l'enrichissement Overpass (plus rapide pour tests)"
    )

    # Export JSON
    parser.add_argument(
        "--save-json",
        action="store_true",
        help="Sauvegarde les résultats en JSON (⚠️  gros fichier !)"
    )

    parser.add_argument(
        "--json-output",
        type=str,
        default="data/output/accidents_enriched.json",
        help="Chemin du fichier JSON de sortie"
    )

    # Elasticsearch
    parser.add_argument(
        "--send-elk",
        action="store_true",
        help="Envoie les données vers Elasticsearch"
    )

    parser.add_argument(
        "--elk-host",
        type=str,
        default="localhost",
        help="Hôte Elasticsearch"
    )

    parser.add_argument(
        "--elk-port",
        type=int,
        default=9200,
        help="Port Elasticsearch"
    )

    parser.add_argument(
    "--elk-user",
    type=str,
    default=os.getenv("ELK_USER"),
    help="Username Elasticsearch"
    )

    parser.add_argument(
        "--elk-password",
        type=str,
        default=os.getenv("ELK_PASSWORD"),
        help="Password Elasticsearch"
    )

    parser.add_argument(
        "--elk-index",
        type=str,
        default="accidents-routiers",
        help="Nom de l'index Elasticsearch"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Taille des batchs pour l'envoi vers ELK"
    )

    # Performances
    parser.add_argument(
        "--n-jobs",
        type=int,
        default=10,
        help="Nombre de workers pour le chargement parallèle BAAC"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mode debug (logs détaillés)"
    )

    return parser.parse_args()

import numpy as np

def convert_to_json_serializable(obj):
    """
    Convertit les types pandas/numpy en types JSON sérialisables.
    """
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat() if pd.notna(obj) else None
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return None if np.isnan(obj) else float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif pd.isna(obj):
        return None
    else:
        return obj

def main():
    args = parse_args()

    # ... (logging config identique) ...

    # 1. Chargement BAAC (nouvelle structure)
    logger.info("\n[1/4] 📥 Chargement des données BAAC...")
    loader = BAACLoader(data_dir=args.data_dir, cache_dir=args.cache_dir)
    data = loader.load_all_years(n_jobs=args.n_jobs, force_reload=args.force_reload)

    df_accidents = data['accidents']
    df_vehicules = data['vehicules']
    df_usagers = data['usagers']

    logger.info(f"✅ {len(df_accidents):,} accidents chargés")

    # 2. Échantillonnage
    if args.sample_size:
        logger.info(f"\n🧪 Échantillonnage de {args.sample_size} accidents")
        sample_ids = df_accidents['num_acc'].sample(min(args.sample_size, len(df_accidents)))
        df_accidents = df_accidents[df_accidents['num_acc'].isin(sample_ids)]
        df_vehicules = df_vehicules[df_vehicules['num_acc'].isin(sample_ids)]
        df_usagers = df_usagers[df_usagers['num_acc'].isin(sample_ids)]
        logger.info(f"✅ {len(df_accidents)} accidents sélectionnés")

    # 3. Initialisation Overpass
    overpass_enricher = None
    if not args.skip_overpass:
        logger.info(f"\n[2/4] 🗺️  Initialisation Overpass sur {args.overpass_url}")
        overpass_enricher = OverpassEnricher(base_url=args.overpass_url)
    else:
        logger.info("\n[2/4] ⏭️  Enrichissement Overpass désactivé")

    # 4. Initialisation ELK
    pusher = None
    if args.send_elk:
        logger.info(f"\n[3/4] 📤 Connexion à Elasticsearch {args.elk_host}:{args.elk_port}")
        try:
            pusher = ElasticPusher(
                host=args.elk_host,
                port=args.elk_port,
                index_name=args.elk_index,
                user=args.elk_user,
                password=args.elk_password
            )
            pusher.create_index_if_not_exists()
            logger.info(f"✅ Index '{args.elk_index}' prêt")
        except Exception as e:
            logger.error(f"❌ Impossible de se connecter à ELK : {e}")
            pusher = None
    else:
        logger.info("\n[3/4] ⏭️  Envoi Elasticsearch désactivé")

    # 5. BOUCLE D'ENRICHISSEMENT AVEC TOUTES LES DONNÉES
    logger.info(f"\n[4/4] ⚙️  Enrichissement...")

    batch = []
    all_results = [] if args.save_json else None

    stats = {
        "total": len(df_accidents),
        "traites": 0,
        "overpass_ok": 0,
        "overpass_ko": 0,
        "sans_gps": 0
    }

    # Groupement véhicules et usagers par accident
    vehicules_by_acc = df_vehicules.groupby('num_acc')
    usagers_by_acc = df_usagers.groupby('num_acc')

    for index, row_acc in tqdm(df_accidents.iterrows(), total=len(df_accidents), desc="Enrichissement"):
        num_acc = row_acc['num_acc']

        # Enrichissement Overpass (avec filtre année)
        infra_data = None
        has_gps = pd.notna(row_acc['lat']) and pd.notna(row_acc['long']) and row_acc['lat'] != 0 and row_acc['long'] != 0

        # Vérifier l'année si filtre activé
        year_ok = True
        if args.overpass_min_year:
            accident_year = row_acc.get('an', 0)
            year_ok = accident_year >= args.overpass_min_year
            if not year_ok:
                logger.debug(f"Accident {num_acc} ({accident_year}) ignoré pour Overpass (< {args.overpass_min_year})")

        if has_gps and overpass_enricher and year_ok:
            try:
                infra_data = overpass_enricher.get_infrastructure(
                    row_acc['lat'],
                    row_acc['long'],
                    radius=args.overpass_radius
                )
                if infra_data:
                    stats["overpass_ok"] += 1
                else:
                    stats["overpass_ko"] += 1
            except Exception as e:
                logger.warning(f"Erreur Overpass pour accident {num_acc}: {e}")
                stats["overpass_ko"] += 1
        elif not has_gps:
            stats["sans_gps"] += 1

        # CONSTRUCTION DU DOCUMENT COMPLET AVEC TOUTES LES COLONNES

        # 1. CARACTÉRISTIQUES + LIEUX (toutes colonnes sauf celles déjà extraites)
        caracteristiques = row_acc.replace({np.nan: None}).to_dict()

        # 2. VÉHICULES (liste complète avec toutes colonnes)
        vehicules_list = []
        if num_acc in vehicules_by_acc.groups:
            for _, veh_row in vehicules_by_acc.get_group(num_acc).iterrows():
                vehicules_list.append(veh_row.replace({np.nan: None}).to_dict())

        # 3. USAGERS (liste complète avec toutes colonnes)
        usagers_list = []
        if num_acc in usagers_by_acc.groups:
            for _, usr_row in usagers_by_acc.get_group(num_acc).iterrows():
                usagers_list.append(usr_row.replace({np.nan: None}).to_dict())

        # 4. DOCUMENT FINAL ENRICHI
        enriched_doc = {
            "id_accident": str(num_acc),
            "timestamp": caracteristiques.get('timestamp').isoformat() if pd.notna(caracteristiques.get('timestamp')) else None,

            # TOUTES les caractéristiques de l'accident (convertir les types)
            "caracteristiques": convert_to_json_serializable(caracteristiques),

            # Liste complète des véhicules (convertir les types)
            "vehicules": convert_to_json_serializable(vehicules_list),

            # Liste complète des usagers (convertir les types)
            "usagers": convert_to_json_serializable(usagers_list),

            # Enrichissement Overpass
            "infrastructure_env": infra_data
        }

        batch.append(enriched_doc)
        stats["traites"] += 1

        if args.save_json:
            all_results.append(enriched_doc)

        # Push batch
        if len(batch) >= args.batch_size:
            if pusher:
                try:
                    pusher.push_documents(batch)
                except Exception as e:
                    logger.error(f"Erreur push batch : {e}")
            batch = []

    # 6. Dernier batch
    if batch and pusher:
        logger.info(f"\n📤 Envoi du dernier batch ({len(batch)} documents)...")
        pusher.push_documents(batch)

    # 7. Sauvegarde JSON
    if args.save_json and all_results:
        logger.info(f"\n💾 Sauvegarde JSON dans {args.json_output}...")
        os.makedirs(os.path.dirname(args.json_output), exist_ok=True)
        with open(args.json_output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ {len(all_results)} accidents sauvegardés")

    # 8. Statistiques
    logger.info("\n" + "="*60)
    logger.info("📊 STATISTIQUES FINALES")
    logger.info("="*60)
    logger.info(f"Total accidents    : {stats['total']:,}")
    logger.info(f"Traités            : {stats['traites']:,}")
    logger.info(f"Overpass OK        : {stats['overpass_ok']:,}")
    logger.info(f"Overpass KO        : {stats['overpass_ko']:,}")
    logger.info(f"Sans GPS           : {stats['sans_gps']:,}")
    logger.info("="*60)
    logger.info("✅ TRAITEMENT TERMINÉ")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.error("\nExécution interrompue par l'utilisateur.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"\nERREUR CRITIQUE : {e}")
        sys.exit(1)