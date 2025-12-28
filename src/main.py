import pandas as pd
from tqdm import tqdm
from baac_loader import BAACLoader
from enrichers import MeteoEnricher, OverpassEnricher
import json
import os

# --- CONFIGURATION ---
SAMPLE_SIZE = 50  # Mettre None pour tout traiter, ou un nombre (ex: 50) pour tester
OUTPUT_FILE = "data/output/accidents_enriched.json"

def main():
    # 1. Chargement BAAC
    loader = BAACLoader()
    df = loader.load_and_merge()

    # 2. Échantillonnage (Pour le dev)
    if SAMPLE_SIZE:
        print(f"🧪 Mode TEST: Traitement de {SAMPLE_SIZE} accidents aléatoires uniquement.")
        df = df.sample(SAMPLE_SIZE)

    results = []

    print("🚀 Démarrage de l'enrichissement...")

    # 3. Boucle d'enrichissement
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):

        acc_data = row.to_dict()

        # Conversion du timestamp pour format string JSON
        date_str = row['timestamp'].strftime('%Y-%m-%d')
        hour = row['timestamp'].hour

        # --- Appel API Météo ---
        meteo_data = MeteoEnricher.get_weather(
            row['lat'],
            row['long'],
            date_str,
            hour
        )

        # --- Appel API Overpass (Infra) ---
        # On ne le fait que si lat/lon sont valides (non NaN)
        infra_data = None
        if pd.notna(row['lat']) and pd.notna(row['long']):
            infra_data = OverpassEnricher.get_infrastructure(
                row['lat'],
                row['long']
            )

        # 4. Construction de l'objet final
        enriched_doc = {
            "id_accident": str(row['Num_Acc']),
            "timestamp": row['timestamp'].isoformat(),
            "location": {
                "lat": row['lat'],
                "lon": row['long'],
                "dep": str(row['dep']),
                "commune": str(row['com'])
            },
            "contexte": {
                "type_route": int(row['catr']), # 1=Autoroute, etc.
                "lum": int(row['lum']),         # Luminosité
                "agglo": int(row['agglo']),
                "gravite_globale": row['gravite_globale']
            },
            "meteo_reelle": meteo_data,
            "infrastructure_env": infra_data,
            "raw_baac": {
                # On garde quelques données brutes utiles
                "col": row.get('col'),
                "int": row.get('int'),
                "atm": row.get('atm') # Météo déclarée par la police (pour comparer avec API)
            }
        }

        results.append(enriched_doc)

    # 5. Sauvegarde JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"✨ Terminé ! {len(results)} accidents enrichis sauvegardés dans {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
