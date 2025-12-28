import requests
import os
import time
from joblib import Memory
from ratelimit import limits, sleep_and_retry
import backoff

# Configuration du cache
memory = Memory("data/cache", verbose=0)

# --- CONFIGURATION RATE LIMITS ---
# Open-Meteo: Max 600 req/min (soit 10 req/sec max) -> on vise 5 req/sec pour être safe
METEO_CALLS = 5
METEO_PERIOD = 1 # seconde

# Overpass: Complexe (slots). On limite arbitrairement pour ne pas saturer.
# On vise 1 requête toutes les 2 secondes pour être très gentil.
OVERPASS_CALLS = 1
OVERPASS_PERIOD = 2 # secondes

class MeteoEnricher:
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    @staticmethod
    @sleep_and_retry                    # Si on dépasse, ça dort auto
    @limits(calls=METEO_CALLS, period=METEO_PERIOD)
    @backoff.on_exception(              # Si 429 ou erreur serveur, on retry auto
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5
    )
    @memory.cache
    def get_weather(lat, lon, date_str, hour):
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_str,
            "end_date": date_str,
            "hourly": "temperature_2m,precipitation,rain,snowfall,visibility,windspeed_10m,weathercode"
        }

        # Plus besoin de time.sleep() manuel ici, les décos gèrent !
        response = requests.get(MeteoEnricher.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        idx = hour
        hourly = data.get("hourly", {})

        # (Le reste du parsing reste identique...)
        return {
            "temp_c": hourly["temperature_2m"][idx],
            # ...
        }

class OverpassEnricher:
    #BASE_URL = "https://overpass-api.de/api/interpreter"
    BASE_URL = "https://overpass.private.coffee/api/interpreter"

    @staticmethod
    @sleep_and_retry
    @limits(calls=OVERPASS_CALLS, period=OVERPASS_PERIOD)
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=8, # On insiste un peu plus pour Overpass
        factor=2     # Attente x2 à chaque échec (2s, 4s, 8s...)
    )
    @memory.cache
    def get_infrastructure(lat, lon, radius=500):
        query = f"""
        [out:json][timeout:25];
        (
          node["highway"="speed_camera"](around:{radius},{lat},{lon});
          way["barrier"="guard_rail"](around:{radius},{lat},{lon});
        );
        out count;
        """

        response = requests.get(OverpassEnricher.BASE_URL, params={'data': query}, timeout=20)

        # Gestion spécifique 429 Overpass (Souvent lié aux slots)
        if response.status_code == 429:
            # On lève une exception pour que @backoff la capture et relance
            raise requests.exceptions.RequestException("Overpass Rate Limit (429)")

        response.raise_for_status()
        data = response.json()

        # (Parsing identique...)
        stats = data.get('elements', [])[0].get('tags', {})
        total = int(stats.get('total', 0))

        return {
            "infra_securite_count": total,
            "has_radar_or_rail": total > 0
        }
