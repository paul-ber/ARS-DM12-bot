import logging
from joblib import Parallel, delayed
from tqdm import tqdm
from elasticsearch.helpers import scan, bulk

logger = logging.getLogger("DM12")


class EnrichmentProcessor:
    """Gère l'enrichissement Overpass en parallèle"""
    
    def __init__(self, overpass_enricher):
        self.overpass_enricher = overpass_enricher
    
    def enrich_accident(self, accident_id, lat, lon, radius=1000):
        """Enrichit un seul accident (appelé en parallèle)"""
        try:
            infra_data = self.overpass_enricher.get_infrastructure(lat, lon, radius=radius)
            if infra_data:
                return accident_id, infra_data, "success"
            else:
                return accident_id, None, "empty"
        except Exception as e:
            logger.debug(f"Erreur Overpass pour {accident_id}: {e}")
            return accident_id, None, "error"
    
    def enrich_batch(self, accidents_list, n_jobs=10):
        """
        Enrichit une liste d'accidents en parallèle
        
        Args:
            accidents_list: Liste de dicts avec {id, lat, lon, radius}
            n_jobs: Nombre de workers parallèles
        
        Returns:
            dict: {accident_id: infra_data or None}
        """
        if not accidents_list:
            logger.info("Aucun accident à enrichir")
            return {}
        
        logger.info(f"🔄 Enrichissement parallèle de {len(accidents_list):,} accidents ({n_jobs} workers)")
        
        # Enrichissement parallèle avec joblib (backend threading pour requêtes I/O)
        results = Parallel(n_jobs=n_jobs, backend='threading', verbose=0)(
            delayed(self.enrich_accident)(a['id'], a['lat'], a['lon'], a.get('radius', 1000))
            for a in tqdm(accidents_list, desc="Enrichissement Overpass")
        )
        
        # Comptage et filtrage
        stats = {"success": 0, "empty": 0, "error": 0}
        enriched_data = {}
        
        for accident_id, infra_data, status in results:
            stats[status] += 1
            if infra_data:
                enriched_data[accident_id] = infra_data
        
        logger.info(f"✅ Enrichissement : {stats['success']:,} OK, {stats['empty']:,} vides, {stats['error']:,} KO")
        
        return enriched_data


def get_accidents_to_enrich(pusher, min_year=None):
    """
    Récupère depuis ELK les accidents qui n'ont pas encore infrastructure_env
    
    Args:
        pusher: Instance ElasticPusher
        min_year: Année minimale (optionnel)
    
    Returns:
        list: [{id, lat, lon}, ...]
    """
    logger.info(f"📥 Récupération des accidents sans infrastructure_env...")
    
    # Query : accidents avec GPS mais sans infrastructure_env
    query = {
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "caracteristiques.lat"}}
                ],
                "must_not": [
                    {"exists": {"field": "infrastructure_env.total"}}
                ]
            }
        },
        "_source": ["id_accident", "caracteristiques.lat", "caracteristiques.long", "caracteristiques.an"]
    }
    
    if min_year:
        query["query"]["bool"]["must"].append(
            {"range": {"caracteristiques.an": {"gte": min_year}}}
        )
    
    accidents = []
    for hit in scan(pusher.es, index=pusher.index_name, query=query):
        src = hit["_source"]
        accidents.append({
            "id": src["id_accident"],
            "lat": src["caracteristiques"]["lat"],
            "lon": src["caracteristiques"]["long"]
        })
    
    logger.info(f"{len(accidents):,} accidents à enrichir trouvés")
    return accidents


def update_elk_with_enrichment(pusher, enriched_data, batch_size=500):
    """
    Met à jour les documents Elasticsearch avec les données d'enrichissement
    
    Args:
        pusher: Instance ElasticPusher
        enriched_data: dict {accident_id: infra_data}
        batch_size: Taille des batchs pour mise à jour
    """
    if not enriched_data:
        logger.info("Aucune donnée à mettre à jour dans ELK")
        return
    
    logger.info(f"Mise à jour de {len(enriched_data):,} documents dans Elasticsearch...")
    
    actions = [
        {
            "_op_type": "update",
            "_index": pusher.index_name,
            "_id": accident_id,
            "doc": {"infrastructure_env": infra_data}
        }
        for accident_id, infra_data in enriched_data.items()
    ]
    
    # Envoi par batchs
    for i in tqdm(range(0, len(actions), batch_size), desc="Mise à jour ELK"):
        batch = actions[i:i+batch_size]
        success, failed = bulk(pusher.es, batch, raise_on_error=False, stats_only=True)
        if failed > 0:
            logger.warning(f"Batch {i//batch_size}: {failed} échecs")
    
    logger.info(f"✅ Mise à jour terminée")