import pandas as pd
from tqdm import tqdm
from baac_loader import BAACLoader
from enrichers import OverpassEnricher
from elk_pusher import ElasticPusher
import json
import os
import argparse
import sys

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

def main():
    args = parse_args()

    # Configuration logging
    if args.verbose:
        logging.getLogger("DM12").setLevel(logging.DEBUG)

    logger.info("="*60)
    logger.info("DÉMARRAGE DU PIPELINE D'ENRICHISSEMENT BAAC")
    logger.info("="*60)
    logger.info(f"Répertoire données : {args.data_dir}")
    logger.info(f"Cache : {args.cache_dir}")
    logger.info(f"Échantillon : {args.sample_size or 'TOUS LES ACCIDENTS'}")
    logger.info(f"Overpass : {args.overpass_url}")
    logger.info(f"Envoi ELK : {args.send_elk}")
    logger.info(f"Sauvegarde JSON : {args.save_json}")
    logger.info("="*60)

    # 1. Chargement BAAC
    logger.info("\n[1/4] Chargement des données BAAC...")
    loader = BAACLoader(data_dir=args.data_dir, cache_dir=args.cache_dir)
    df = loader.load_all_years(n_jobs=args.n_jobs, force_reload=args.force_reload)

    logger.info(f"{len(df):,} accidents chargés")

    # 2. Échantillonnage (si activé)
    if args.sample_size:
        logger.info(f"\nMode TEST : échantillonnage de {args.sample_size} accidents")
        df = df.sample(min(args.sample_size, len(df)))
        logger.info(f"{len(df)} accidents sélectionnés")

    # 3. Initialisation Overpass
    overpass_enricher = None
    if not args.skip_overpass:
        logger.info(f"\n[2/4] Initialisation Overpass sur {args.overpass_url}")
        overpass_enricher = OverpassEnricher(base_url=args.overpass_url)
    else:
        logger.info("\n[2/4] Enrichissement Overpass désactivé")

    # 4. Initialisation ELK (avant la boucle)
    pusher = None
    if args.send_elk:
        logger.info(f"\n[3/4] Connexion à Elasticsearch {args.elk_host}:{args.elk_port}")
        try:
            pusher = ElasticPusher(
                host=args.elk_host,
                port=args.elk_port,
                index_name=args.elk_index
            )
            pusher.create_index_if_not_exists()
            logger.info(f"Index '{args.elk_index}' prêt")
        except Exception as e:
            logger.error(f"Impossible de se connecter à ELK : {e}")
            logger.warning("   → Basculement en mode sans ELK")
            pusher = None
    else:
        logger.info("\n[3/4] Envoi Elasticsearch désactivé")

    # 5. Boucle d'enrichissement avec batch
    logger.info(f"\n[4/4] Enrichissement et traitement...")
    logger.info(f"   Batch size : {args.batch_size}")

    batch = []
    all_results = [] if args.save_json else None

    stats = {
        "total": len(df),
        "traites": 0,
        "overpass_ok": 0,
        "overpass_ko": 0,
        "sans_gps": 0
    }

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Enrichissement"):

        # Enrichissement Overpass
        infra_data = None
        has_gps = pd.notna(row['lat']) and pd.notna(row['long']) and row['lat'] != 0 and row['long'] != 0

        if has_gps and overpass_enricher:
            try:
                infra_data = overpass_enricher.get_infrastructure(
                    row['lat'],
                    row['long'],
                    radius=args.overpass_radius
                )
                if infra_data:
                    stats["overpass_ok"] += 1
                else:
                    stats["overpass_ko"] += 1
            except Exception as e:
                logger.warning(f"Erreur Overpass pour accident {row['num_acc']}: {e}")
                stats["overpass_ko"] += 1
        elif not has_gps:
            stats["sans_gps"] += 1

        # Construction du document enrichi
        enriched_doc = {
            "id_accident": str(row.get('num_acc', 'inconnu')),
            "timestamp": row.get('timestamp', pd.Timestamp('2000-01-01')).isoformat(),
            "location": {
                "lat": float(row['lat']) if pd.notna(row['lat']) else None,
                "lon": float(row['long']) if pd.notna(row['long']) else None,
                "dep": str(row.get('dep', 'inconnu')),
                "commune": str(row.get('com', 'inconnu'))
            },
            "contexte": {
                "type_route": int(row.get('catr', 0)),
                "lum": int(row.get('lum', 0)),
                "agglo": int(row.get('agglo', row.get('agg', 0))),
                "gravite_globale": row.get('gravite_accident', 'Inconnu'),
                "nb_tues": int(row.get('nb_tues', 0)),
                "nb_graves": int(row.get('nb_graves', 0))
            },
            "infrastructure_env": infra_data,
            "vehicules": {
                "nb_vehicules": int(row.get('nb_vehicules', 0)),
                "implique_moto": bool(row.get('implique_moto', False)),
                "implique_pl": bool(row.get('implique_pl', False)),
                "implique_velo": bool(row.get('implique_velo', False))
            },
            "raw_baac": {
                "col": int(row.get('col', 0)) if pd.notna(row.get('col')) else None,
                "int": int(row.get('int', 0)) if pd.notna(row.get('int')) else None,
                "atm": int(row.get('atm', 0)) if pd.notna(row.get('atm')) else None
            }
        }

        batch.append(enriched_doc)
        stats["traites"] += 1

        if args.save_json:
            all_results.append(enriched_doc)

        # Push dès qu'on atteint BATCH_SIZE
        if len(batch) >= args.batch_size:
            if pusher:
                try:
                    pusher.push_documents(batch)
                except Exception as e:
                    logger.error(f"Erreur push batch : {e}")
            batch = []

    # 6. Envoi du dernier batch (qui est < BATCH_SIZE)
    if batch and pusher:
        logger.info(f"\nEnvoi du dernier batch ({len(batch)} documents)...")
        pusher.push_documents(batch)

    # 7. Sauvegarde JSON finale (optionnelle)
    if args.save_json and all_results:
        logger.info(f"\nSauvegarde JSON dans {args.json_output}...")
        os.makedirs(os.path.dirname(args.json_output), exist_ok=True)
        with open(args.json_output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ {len(all_results)} accidents sauvegardés")

    # 8. Statistiques finales
    logger.info("\n" + "="*60)
    logger.info("STATISTIQUES FINALES")
    logger.info("="*60)
    logger.info(f"Total accidents    : {stats['total']:,}")
    logger.info(f"Traités            : {stats['traites']:,}")
    logger.info(f"Overpass OK        : {stats['overpass_ok']:,}")
    logger.info(f"Overpass KO        : {stats['overpass_ko']:,}")
    logger.info(f"Sans GPS           : {stats['sans_gps']:,}")
    logger.info(f"JSON sauvegardé    : {'OUI' if args.save_json else 'NON'}")
    logger.info(f"Envoyé vers ELK    : {'OUI' if pusher else 'NON'}")
    logger.info("="*60)
    logger.info("TRAITEMENT TERMINÉ")
    logger.info("="*60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.error("\nExécution interrompue par l'utilisateur.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"\nERREUR CRITIQUE : {e}")
        sys.exit(1)