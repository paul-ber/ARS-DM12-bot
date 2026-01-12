import requests
import time
from joblib import Memory
from ratelimit import limits, sleep_and_retry
import backoff
import logging

logger = logging.getLogger("DM12")
memory = Memory("data/cache", verbose=0)

METEO_CALLS = 5
METEO_PERIOD = 1
OVERPASS_CALLS = 1
OVERPASS_PERIOD = 2

class MeteoEnricher:
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    @staticmethod
    @sleep_and_retry
    @limits(calls=METEO_CALLS, period=METEO_PERIOD)
    @backoff.on_exception(
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

        response = requests.get(MeteoEnricher.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        idx = hour
        hourly = data.get("hourly", {})

        return {
            "temp_c": hourly["temperature_2m"][idx],
            "precip_mm": hourly["precipitation"][idx],
            "rain_mm": hourly["rain"][idx],
            "snow_cm": hourly["snowfall"][idx],
            "visibility_m": hourly.get("visibility", [None]*24)[idx],
            "wind_kmh": hourly["windspeed_10m"][idx],
            "weather_code": hourly["weathercode"][idx]
        }

class OverpassEnricher:
    BASE_URL = "https://overpass.private.coffee/api/interpreter"
    #"https://overpass.private.coffee/api/interpreter"

    @staticmethod
    @sleep_and_retry
    @limits(calls=OVERPASS_CALLS, period=OVERPASS_PERIOD)
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=8,
        factor=2
    )
    @memory.cache
    def get_infrastructure(lat, lon, radius=1000):
        """
        Récupère les infrastructures routières dans un rayon donné.
        Rayon élargi à 1000m pour couvrir plus de zones.
        """
        logger.debug(f"🔍 Overpass query: lat={lat}, lon={lon}, radius={radius}m")

        # Requête enrichie avec plus de types d'infrastructures
        query = f"""
        [out:json][timeout:30];
        (
          /* Sécurité routière */
          node["highway"="speed_camera"](around:{radius},{lat},{lon});
          way["barrier"="guard_rail"](around:{radius},{lat},{lon});
          node["traffic_calming"](around:{radius},{lat},{lon});
          node["highway"="traffic_signals"](around:{radius},{lat},{lon});
          node["highway"="stop"](around:{radius},{lat},{lon});
          node["highway"="give_way"](around:{radius},{lat},{lon});

          /* Passages piétons */
          node["highway"="crossing"](around:{radius},{lat},{lon});

          /* Ronds-points et jonctions */
          way["junction"="roundabout"](around:{radius},{lat},{lon});

          /* Routes principales avec infos vitesse */
          way["highway"~"^(motorway|trunk|primary|secondary)$"]["maxspeed"](around:{radius},{lat},{lon});
        );
        out geom;
        """

        try:
            response = requests.get(
                OverpassEnricher.BASE_URL,
                params={'data': query},
                timeout=35
            )

            logger.debug(f"   → Status: {response.status_code}")

            if response.status_code == 429:
                logger.warning("⏳ Overpass Rate Limit 429, retry...")
                raise requests.exceptions.RequestException("Rate Limit")

            if response.status_code == 504:
                logger.warning("⏳ Overpass Timeout 504, zone trop chargée")
                return None

            response.raise_for_status()
            data = response.json()

            elements = data.get('elements', [])

            if not elements:
                logger.debug("   → Zone vide (pas d'infrastructure OSM)")
                return {
                    "radars": 0,
                    "glissieres": 0,
                    "ralentisseurs": 0,
                    "feux": 0,
                    "stops": 0,
                    "passages_pietons": 0,
                    "ronds_points": 0,
                    "routes_principales": 0,
                    "vitesse_max_moyenne": None,
                    "total": 0
                }

            # Comptage par type
            radars = sum(1 for e in elements if e.get('tags', {}).get('highway') == 'speed_camera')
            glissieres = sum(1 for e in elements if e.get('tags', {}).get('barrier') == 'guard_rail')
            ralentisseurs = sum(1 for e in elements if 'traffic_calming' in e.get('tags', {}))
            feux = sum(1 for e in elements if e.get('tags', {}).get('highway') == 'traffic_signals')
            stops = sum(1 for e in elements if e.get('tags', {}).get('highway') == 'stop')
            cedez = sum(1 for e in elements if e.get('tags', {}).get('highway') == 'give_way')
            crossings = sum(1 for e in elements if e.get('tags', {}).get('highway') == 'crossing')
            roundabouts = sum(1 for e in elements if e.get('tags', {}).get('junction') == 'roundabout')

            # Routes avec vitesse
            speeds = []
            routes_principales = 0
            for e in elements:
                tags = e.get('tags', {})
                if tags.get('highway') in ['motorway', 'trunk', 'primary', 'secondary']:
                    routes_principales += 1
                    maxspeed = tags.get('maxspeed', '')
                    # Parse "50", "50 km/h", etc.
                    try:
                        speed_val = int(''.join(filter(str.isdigit, maxspeed)))
                        if 20 <= speed_val <= 150:  # Valeurs plausibles
                            speeds.append(speed_val)
                    except:
                        pass

            vitesse_moy = round(sum(speeds) / len(speeds)) if speeds else None

            result = {
                "radars": radars,
                "glissieres": glissieres,
                "ralentisseurs": ralentisseurs,
                "feux": feux,
                "stops_cedez": stops + cedez,
                "passages_pietons": crossings,
                "ronds_points": roundabouts,
                "routes_principales": routes_principales,
                "vitesse_max_moyenne": vitesse_moy,
                "total": len(elements)
            }

            logger.debug(f"   ✓ Trouvé: {result}")
            return result

        except Exception as e:
            logger.error(f"❌ Erreur Overpass ({lat}, {lon}): {e}")
            return None
