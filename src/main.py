import pandas as pd
from tqdm import tqdm
from baac_loader import BAACLoader
from enrichers import MeteoEnricher, OverpassEnricher
from elk_pusher import ElasticPusher
import json
import os

# --- LOGGING GLOBAL ---
import logging
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(processName)s %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),                # Console
        logging.FileHandler(LOG_FILE, encoding="utf-8")  # Fichier
    ],
)
logger = logging.getLogger("DM12")

# --- CONFIGURATION ---
SAMPLE_SIZE = 5  # None = Tout charger
OUTPUT_JSON = True  # Sauvegarder aussi en JSON ?
OUTPUT_FILE = "data/output/accidents_enriched.json"

SEND_TO_ELK = False
ELK_HOST = "localhost"
ELK_PORT = 9200

BATCH_SIZE = 100  # Envoie tous les 100 accidents

def main():
    # 1. Chargement BAAC
    loader = BAACLoader()
    df = loader.load_all_years()

    # 2. Échantillonnage (si activé)
    if SAMPLE_SIZE:
        print(f"🧪 Mode TEST: {SAMPLE_SIZE} accidents")
        df = df.sample(SAMPLE_SIZE)

    # 3. Initialisation ELK (avant la boucle)
    pusher = None
    if SEND_TO_ELK:
        try:
            pusher = ElasticPusher(host=ELK_HOST, port=ELK_PORT)
            pusher.create_index_if_not_exists()
        except Exception as e:
            print(f"Impossible de se connecter à ELK : {e}")
            print("   → Basculement en mode JSON uniquement.")
            pusher = None

    # 4. Boucle avec batch
    batch = []
    all_results = [] if OUTPUT_JSON else None  # Pour le fichier JSON final (optionnel)
    
    print("Enrichissement et envoi par batch...")

    for index, row in tqdm(df.iterrows(), total=len(df)):
        
        # Enrichissement (identique)
        date_str = row['timestamp'].strftime('%Y-%m-%d')
        hour = row['timestamp'].hour

        meteo_data = None
        if pd.notna(row['lat']) and pd.notna(row['long']) and row['lat'] != 0 and row['long'] != 0:
            try:
                meteo_data = MeteoEnricher.get_weather(row['lat'], row['long'], date_str, hour)
            except Exception as e:
                logger.warning(f"Erreur météo pour accident {row['num_acc']}: {e}")
        else:
            logger.debug(f"Pas de GPS pour accident {row['num_acc']}, météo ignorée")

        infra_data = None
        if pd.notna(row['lat']) and pd.notna(row['long']) and row['lat'] != 0 and row['long'] != 0:
            try:
                infra_data = OverpassEnricher.get_infrastructure(row['lat'], row['long'])
            except Exception as e:
                logger.warning(f"Erreur Overpass pour accident {row['num_acc']}: {e}")

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
                "gravite_globale": row.get('gravite_accident', 'Inconnu')
            },
            "meteo_reelle": meteo_data,
            "infrastructure_env": infra_data,
            "raw_baac": {
                "col": row.get('col'),
                "int": row.get('int'),
                "atm": row.get('atm')
            }
        }
        
        batch.append(enriched_doc)
        
        if OUTPUT_JSON:
            all_results.append(enriched_doc)

        #  Push dès qu'on atteint BATCH_SIZE
        if len(batch) >= BATCH_SIZE:
            if pusher:
                try:
                    pusher.push_documents(batch)
                except Exception as e:
                    print(f"Erreur push batch : {e}")
            
            batch = []  # Vide le batch

    # 5. Envoi du dernier batch (qui est < BATCH_SIZE)
    if batch and pusher:
        pusher.push_documents(batch)

    # 6. Sauvegarde JSON finale (optionnelle)
    if OUTPUT_JSON and all_results:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"\n{len(all_results)} accidents sauvegardés dans {OUTPUT_FILE}")

    print("\nTraitement terminé.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.error("Exécution interrompue par l'utilisateur.")