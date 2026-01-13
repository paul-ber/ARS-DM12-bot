from elasticsearch import Elasticsearch, helpers
import logging

logger = logging.getLogger("DM12")

class ElasticPusher:
    def __init__(self, host="localhost", port=9200, index_name="accidents-routiers",
                 user=None, password=None):
        """
        Initialise la connexion à Elasticsearch avec authentification optionnelle.
        """
        if user and password:
            self.es = Elasticsearch(
                [f"http://{host}:{port}"],
                basic_auth=(user, password)
            )
        else:
            self.es = Elasticsearch([f"http://{host}:{port}"])

        self.index_name = index_name

        # Test connexion
        if not self.es.ping():
            raise ConnectionError(f"❌ Impossible de se connecter à Elasticsearch sur {host}:{port}")

        info = self.es.info()
        logger.info(f"✅ Connecté à Elasticsearch : {info['version']['number']}")

    def create_index_if_not_exists(self):
        """
        Crée l'index avec mapping optimisé pour données BAAC complètes.
        """
        if self.es.indices.exists(index=self.index_name):
            logger.info(f"ℹ️  Index '{self.index_name}' existe déjà.")
            return

        mapping = {
            "mappings": {
                "properties": {
                    "id_accident": {"type": "keyword"},
                    "timestamp": {"type": "date"},

                    # CARACTERISTIQUES (objet flat avec tous les champs)
                    "caracteristiques": {
                        "properties": {
                            "lat": {"type": "float"},
                            "long": {"type": "float"},
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
                            "adr": {"type": "text"}
                        }
                    },

                    # VÉHICULES (nested array)
                    "vehicules": {
                        "type": "nested",
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
                    },

                    # USAGERS (nested array)
                    "usagers": {
                        "type": "nested",
                        "properties": {
                            "num_acc": {"type": "keyword"},
                            "id_vehicule": {"type": "keyword"},
                            "num_veh": {"type": "keyword"},
                            "place": {"type": "integer"},
                            "catu": {"type": "integer"},
                            "grav": {"type": "integer"},
                            "sexe": {"type": "integer"},
                            "an_nais": {"type": "integer"},
                            "trajet": {"type": "integer"},
                            "secu1": {"type": "integer"},
                            "secu2": {"type": "integer"},
                            "secu3": {"type": "integer"},
                            "locp": {"type": "integer"},
                            "actp": {"type": "keyword"},
                            "etatp": {"type": "integer"}
                        }
                    },

                    # INFRASTRUCTURE OSM
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

        self.es.indices.create(index=self.index_name, body=mapping)
        logger.info(f"✅ Index '{self.index_name}' créé avec mapping complet.")

    def push_documents(self, documents):
        """
        Envoie une liste de documents (dicts Python) vers Elasticsearch en bulk.
        """
        # Transformation : Kibana veut 'geo_point' sous format {"lat": X, "lon": Y}
        for doc in documents:
            if "caracteristiques" in doc and "lat" in doc["caracteristiques"] and "lon" in doc["caracteristiques"]:
                lat = doc["caracteristiques"]["lat"]
                lon = doc["caracteristiques"]["long"]
                if lat is not None and lon is not None:
                    doc["caracteristiques"]["coords"] = {
                        "lat": lat,
                        "lon": lon
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
        logger.debug(f"📤 Push : {success} OK, {failed} KO")
        return success, failed
