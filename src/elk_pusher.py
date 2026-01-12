from elasticsearch import Elasticsearch, helpers
import json

class ElasticPusher:
    def __init__(self, host="localhost", port=9200, index_name="accidents-routiers"):
        """
        Initialise la connexion à Elasticsearch.
        """
        self.es = Elasticsearch([f"http://{host}:{port}"])
        self.index_name = index_name

        # Test connexion
        if not self.es.ping():
            raise ConnectionError(f"❌ Impossible de se connecter à Elasticsearch sur {host}:{port}")
        print(f"✅ Connecté à Elasticsearch : {self.es.info()['version']['number']}")

    def create_index_if_not_exists(self):
        """
        Crée l'index avec un mapping optimisé si il n'existe pas.
        """
        if self.es.indices.exists(index=self.index_name):
            print(f"ℹ️  Index '{self.index_name}' existe déjà.")
            return

        # Mapping optimisé pour Kibana
        mapping = {
            "mappings": {
                "properties": {
                    "id_accident": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "location": {
                        "properties": {
                            "coords": {"type": "geo_point"},  # CRUCIAL pour les cartes Kibana
                            "lat": {"type": "float"},
                            "lon": {"type": "float"},
                            "dep": {"type": "keyword"},
                            "commune": {"type": "keyword"}
                        }
                    },
                    "contexte": {
                        "properties": {
                            "type_route": {"type": "integer"},
                            "lum": {"type": "integer"},
                            "agglo": {"type": "integer"},
                            "gravite_globale": {"type": "keyword"}
                        }
                    },
                    "meteo_reelle": {
                        "properties": {
                            "temp_c": {"type": "float"},
                            "precip_mm": {"type": "float"},
                            "rain_mm": {"type": "float"},
                            "snow_cm": {"type": "float"},
                            "visibility_m": {"type": "float"},
                            "wind_kmh": {"type": "float"},
                            "weather_code": {"type": "integer"}
                        }
                    },
                    "infrastructure_env": {
                        "properties": {
                            "infra_securite_count": {"type": "integer"},
                            "has_radar_or_rail": {"type": "boolean"}
                        }
                    }
                }
            }
        }

        self.es.indices.create(index=self.index_name, body=mapping)
        print(f"✅ Index '{self.index_name}' créé avec mapping optimisé.")

    def push_documents(self, documents):
        """
        Envoie une liste de documents (dicts Python) vers Elasticsearch en bulk.
        """
        # Transformation : Kibana veut 'geo_point' sous format {"lat": X, "lon": Y}
        for doc in documents:
            if "location" in doc and "lat" in doc["location"] and "lon" in doc["location"]:
                doc["location"]["coords"] = {
                    "lat": doc["location"]["lat"],
                    "lon": doc["location"]["lon"]
                }

        # Bulk insert (performant)
        actions = [
            {
                "_index": self.index_name,
                "_id": doc["id_accident"],  # Utilise l'ID accident comme clé unique
                "_source": doc
            }
            for doc in documents
        ]

        success, failed = helpers.bulk(self.es, actions, stats_only=True)
        print(f"📤 Push terminé : {success} documents envoyés, {failed} échecs.")
        return success, failed
