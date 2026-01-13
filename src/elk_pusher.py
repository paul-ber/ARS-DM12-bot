from elasticsearch import Elasticsearch, helpers
import logging

logger = logging.getLogger("DM12")

class ElasticPusher:
    def __init__(self, host="localhost", port=9200, user=None, password=None):
        """Initialise la connexion à Elasticsearch"""
        if user and password:
            self.es = Elasticsearch(
                [f"http://{host}:{port}"],
                basic_auth=(user, password)
            )
        else:
            self.es = Elasticsearch([f"http://{host}:{port}"])

        # Test connexion
        if not self.es.ping():
            raise ConnectionError(f"❌ Impossible de se connecter à Elasticsearch sur {host}:{port}")

        info = self.es.info()
        logger.info(f"Connecté à Elasticsearch : {info['version']['number']}")

    def create_accidents_index(self, index_name="accidents-routiers"):
        """Crée l'index des accidents avec mapping optimisé"""
        if self.es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' existe déjà")
            return

        mapping = {
            "mappings": {
                "properties": {
                    "num_acc": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "an": {"type": "integer"},
                    "mois": {"type": "integer"},
                    "jour": {"type": "integer"},
                    "heure": {"type": "integer"},
                    "lat": {"type": "float"},
                    "long": {"type": "float"},
                    "coords": {"type": "geo_point"},
                    "dep": {"type": "keyword"},
                    "com": {"type": "keyword"},
                    "agg": {"type": "integer"},
                    "int": {"type": "integer"},
                    "atm": {"type": "integer"},
                    "col": {"type": "integer"},
                    "lum": {"type": "integer"},
                    "catr": {"type": "integer"},
                    "circ": {"type": "integer"},
                    "nbv": {"type": "integer"},
                    "vosp": {"type": "integer"},
                    "prof": {"type": "integer"},
                    "plan": {"type": "integer"},
                    "surf": {"type": "integer"},
                    "infra": {"type": "integer"},
                    "situ": {"type": "integer"},
                    "vma": {"type": "integer"},
                    "adr": {"type": "text"},
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
                    }
                }
            }
        }

        self.es.indices.create(index=index_name, body=mapping)
        logger.info(f"Index '{index_name}' créé")

    def create_vehicules_index(self, index_name="accidents-vehicules"):
        """Crée l'index des véhicules"""
        if self.es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' existe déjà")
            return

        mapping = {
            "mappings": {
                "properties": {
                    "num_acc": {"type": "keyword"},
                    "id_vehicule": {"type": "keyword"},
                    "num_veh": {"type": "keyword"},
                    "senc": {"type": "integer"},
                    "catv": {"type": "integer"},
                    "obs": {"type": "integer"},
                    "obsm": {"type": "integer"},
                    "choc": {"type": "integer"},
                    "manv": {"type": "integer"},
                    "motor": {"type": "integer"},
                    "occutc": {"type": "integer"}
                }
            }
        }

        self.es.indices.create(index=index_name, body=mapping)
        logger.info(f"Index '{index_name}' créé")

    def create_usagers_index(self, index_name="accidents-usagers"):
        """Crée l'index des usagers"""
        if self.es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' existe déjà")
            return

        mapping = {
            "mappings": {
                "properties": {
                    "num_acc": {"type": "keyword"},
                    "id_vehicule": {"type": "keyword"},
                    "num_veh": {"type": "keyword"},
                    "place": {"type": "integer"},
                    "catu": {"type": "integer"},
                    "grav": {"type": "integer"},
                    "sexe": {"type": "integer"},
                    "an_nais": {"type": "integer"},
                    "age": {"type": "integer"},
                    "trajet": {"type": "integer"},
                    "secu1": {"type": "integer"},
                    "secu2": {"type": "integer"},
                    "secu3": {"type": "integer"},
                    "locp": {"type": "integer"},
                    "actp": {"type": "keyword"},
                    "etatp": {"type": "integer"}
                }
            }
        }

        self.es.indices.create(index=index_name, body=mapping)
        logger.info(f"Index '{index_name}' créé")

    def push_documents(self, documents, index_name):
        """Envoie des documents vers un index spécifique"""
        if not documents:
            return 0, 0

        actions = [
            {
                "_index": index_name,
                "_id": doc.get("num_acc") if index_name == "accidents-routiers" else None,
                "_source": doc
            }
            for doc in documents
        ]

        success, failed = helpers.bulk(self.es, actions, stats_only=True, raise_on_error=False)
        logger.debug(f"{index_name}: {success} OK, {failed} KO")
        return success, failed
