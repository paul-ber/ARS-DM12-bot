from elasticsearch import Elasticsearch, helpers
import logging

logger = logging.getLogger("DM12")

class ElasticPusher:
    def __init__(self, host="localhost", port=9200, index_name="accidents-routiers"):
        """
        Initialise la connexion à Elasticsearch.
        """
        self.es = Elasticsearch([f"http://{host}:{port}"])
        self.index_name = index_name

        # Test connexion
        if not self.es.ping():
            raise ConnectionError(f"Impossible de se connecter à Elasticsearch sur {host}:{port}")

        info = self.es.info()
        logger.info(f"Connecté à Elasticsearch : {info['version']['number']}")

    def create_index_if_not_exists(self):
        """
        Crée l'index avec un mapping optimisé si il n'existe pas.
        """
        if self.es.indices.exists(index=self.index_name):
            logger.info(f"Index '{self.index_name}' existe déjà.")
            return

        # Mapping optimisé pour Kibana
        mapping = {
            "mappings": {
                "properties": {
                    "id_accident": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "location": {
                        "properties": {
                            "coords": {"type": "geo_point"},
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
                            "gravite_globale": {"type": "keyword"},
                            "nb_tues": {"type": "integer"},
                            "nb_graves": {"type": "integer"}
                        }
                    },
                    "infrastructure_env": {
                        "properties": {
                            "radars": {"type": "integer"},
                            "glissieres": {"type": "integer"},
                            "ralentisseurs": {"type": "integer"},
                            "feux": {"type": "integer"},
                            "stops_cedez": {"type": "integer"},
                            "passages_pietons": {"type": "integer"},
                            "ronds_points": {"type": "integer"},
                            "routes_principales": {"type": "integer"},
                            "vitesse_max_moyenne": {"type": "integer"},
                            "total": {"type": "integer"}
                        }
                    },
                    "vehicules": {
                        "properties": {
                            "nb_vehicules": {"type": "integer"},
                            "implique_moto": {"type": "boolean"},
                            "implique_pl": {"type": "boolean"},
                            "implique_velo": {"type": "boolean"}
                        }
                    },
                    "raw_baac": {
                        "properties": {
                            "col": {"type": "integer"},
                            "int": {"type": "integer"},
                            "atm": {"type": "integer"}
                        }
                    }
                }
            }
        }

        self.es.indices.create(index=self.index_name, body=mapping)
        logger.info(f"Index '{self.index_name}' créé avec mapping optimisé.")

    def push_documents(self, documents):
        """
        Envoie une liste de documents (dicts Python) vers Elasticsearch en bulk.
        """
        # Transformation : Kibana veut 'geo_point' sous format {"lat": X, "lon": Y}
        for doc in documents:
            if "location" in doc and "lat" in doc["location"] and "lon" in doc["location"]:
                if doc["location"]["lat"] is not None and doc["location"]["lon"] is not None:
                    doc["location"]["coords"] = {
                        "lat": doc["location"]["lat"],
                        "lon": doc["location"]["lon"]
                    }

        # Bulk insert (performant)
        actions = [
            {
                "_index": self.index_name,
                "_id": doc["id_accident"],
                "_source": doc
            }
            for doc in documents
        ]

        success, failed = helpers.bulk(self.es, actions, stats_only=True, raise_on_error=False)
        logger.debug(f"Push : {success} OK, {failed} KO")
        return success, failed
