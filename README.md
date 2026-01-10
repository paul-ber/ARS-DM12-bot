# ARS-DM12 BOT : Analyse des Accidents Routiers : Corrélation Infrastructure & Sécurité

### Objectif général

Analyser la **cartographie spatiale et temporelle des accidents routiers en France** en croisant des données multiples pour identifier les corrélations entre :

- **Infrastructure de sécurité** (radars, glissières de sécurité, limitation de vitesse)
- **Conditions météorologiques** (pluie, neige, vent, visibilité)
- **Contexte temporel** (heure, jour de semaine, saison)
- **Topographie routière** (type de route, densité de circulation)
- **Profil des victimes** (âge, type de véhicule)

### Problématique centrale

**Démonter que les accidents routiers mortels en France sont corrélés à l'absence d'infrastructures de sécurité adéquates, amplifiée par des conditions météorologiques adverses, et que cette corrélation varie fortement par région.**

## Datasets utilisés : Sélection et Justification

### 1. **Base des Accidents Corporels (BAAC) - [data.gouv.fr](http://data.gouv.fr/)**

**URL** : `https://www.data.gouv.fr/fr/datasets/accidents-corporels-de-la-circulation-routiere/` 

**Fréquence de mise à jour** : Annuelle (données N-1, ex: 2023 disponible fin 2024)

**Structure de données** :
La base BAAC est composée de **3 fichiers CSV complémentaires** par année :

### a) Fichier "Caractéristiques" (un enregistrement = un accident)

| Colonne | Type | Importance | Exemple |
| --- | --- | --- | --- |
| `num_acc` | STRING | 🔴 CLEF PRIMARY | "202400000123" |
| `jour` | INT | ⭐ Jour semaine (1-7) | 5 = vendredi |
| `mois` | INT | ⭐ Saison | 12 = décembre |
| `an` | INT | ⭐ Trend temporelle | 2023 |
| `hrmn` | STRING | 🔴 Heure exacte | "14:30" |
| `lat` | FLOAT | 🔴 GÉOCODE | 48.8566 |
| `long` | FLOAT | 🔴 GÉOCODE | 2.3522 |
| `dep` | STRING | Département | 75 = Paris |
| `com` | STRING | Commune INSEE | 75056 |
| `adr` | STRING | Adresse (libre) | "Avenue des Champs-Élysées" |
| `col` | INT | Type de collision | 1=Arrière, 2=Latéral, 3=Frontal |
| `agglo` | INT | Agglomération | 1=Oui, 2=Non |
| `route_type` | INT | Route urbaine/RN/Autoroute | 1, 2, 3 |
| `luminosite` | INT | 1=Plein jour, 2=Crépuscule, 3=Nuit | ⭐⭐ Vis. lumineuse |
| `conditions_meteo` | INT | Codes météo | 1=Normal, 2=Pluie, 3=Neige, 4=Brouillard, 5=Vent |
| `etat_surface` | INT | État route | 1=Sèche, 2=Mouillée, 3=Flaque, 4=Inondée, 5=Enneigée |

### b) Fichier "Lieux" (description du lieu, lié par `num_acc`)

| Colonne | Importance |
| --- | --- |
| `num_acc` | 🔴 Clef étrangère BAAC |
| `v1` | Largeur chaussée |
| `v2` | Rayon de courbure |
| `v3` | Pente route |
| `V4` | Intersection (signalisation) |
| `V5` | Accès propriété |
| `V6` | Type intersection |

### c) Fichier "Usagers" (passagers/conducteurs, multiple par accident)

| Colonne | Importance |
| --- | --- |
| `num_acc` | 🔴 Clef étrangère |
| `num_veh` | Numéro véhicule |
| `place` | Position dans véhicule |
| `categorie_usager` | 1=Driver, 2=Passenger, 3=Piéton |
| `sexe` | 1=Masculin, 2=Féminin |
| `an_naissance` | Age calculé |
| `gravite` | 🔴🔴🔴 **1=Indemne, 2=Blessé léger, 3=Blessé hospitalisé, 4=Tué** |
| `type_usager` | Conducteur/passager/piéton |

**Richesse de la base** :

- **~60 000 accidents/an** en France entre 2018-2023
- **~70 000 blessés graves/mortels/an**
- **~900 000 lignes usagers** = grain fin pour analyse démographique

**Critères de sélection** :
✅ Données publiques officielles (ONISR - Observatoire National Sécurité Routière)
✅ Couvre 15+ ans (trend temporelle robuste)
✅ Géolocalisation GPS précise (lat/lon)
✅ Horodatage précis (heure + minute)
✅ Variables multidimensionnelles (routes, météo, conducteurs, gravité)

---

### 2. **Meteorological Data - Open-Meteo Historical Weather API**

**URL** : `https://open-meteo.com/en/docs/historical-weather-api` [web:79][web:76]

**Fréquence de mise à jour** : Historique complet depuis 1940

**Variables disponibles** (pour notre analyse) :

| Variable | Unité | Importance | Exemple |
| --- | --- | --- | --- |
| `temperature_2m` | °C | ⭐ Adhérence route | 2, 25, -5 |
| `relative_humidity_2m` | % | ⭐ Brouillard/givre | 45, 95 |
| `precipitation` | mm | 🔴🔴 CRITIQUE | 0, 12.5, 50 |
| `weathercode` | Code WMO | ⭐⭐ Type météo | 0=Clear, 45=Foggy, 61=Rainy, 71=Snowy |
| `windspeed_10m` | km/h | ⭐ Stabilité véhicule | 5, 35, 65 |
| `visibility` | m | ⭐⭐ Visibilité directe | 10000, 500, 50 |

**Données horaires** : Résolution 1 heure (ICON-D2 pour France = 2km de précision spatiale)

**Procédure d'enrichissement** :

```
Pour chaque accident (lat, lon, date, heure):
  1. Appel API : GET /v1/archive?latitude={lat}&longitude={lon}&date={YYYY-MM-DD}&hourly=...
  2. Récupère données heure exacte de l'accident
  3. Stocke variables météo dans JSON enrichi
```

**Critères de sélection** :
✅ API gratuite (pas de quota limité)
✅ Données historiques depuis 1940 (couverture complète BAAC 2005-2024)
✅ Résolution horaire = corrélation exacte avec heure accident
✅ Bien documentée avec Python wrapper [web:73][web:82]
✅ Alternative : Météo-France API (payante) = fallback si besoin validation

---

### 3. **Infrastructures Routières - Overpass API (OpenStreetMap)**

**URL** : `https://overpass-api.de/api/interpreter` [web:51]

**Type de requête** : Query Overpass Language (QL)

**Éléments recherchés** (dans rayon 500-1000m de chaque accident) :

### a) Radars de vitesse [web:80]

```
[out:json];
node["highway"="speed_camera"](bbox);
out center;

```

**Tags OSM** :

- `highway=speed_camera` : Radar fixe
- `enforcement=photo_speed` : Radars feu rouge
- `maxspeed=*` : Limitation vitesse associée

### b) Infrastructures de sécurité

```
[out:json];
(
  way["barrier"="guard_rail"](bbox);
  way["man_made"="guard_rail"](bbox);
  node["traffic_signals"](bbox);
  node["amenity"="fuel"](bbox);  /* Arrêts pour fatigue */
);
out geom;
```

| OSM Tag | Signification | Importance |
| --- | --- | --- |
| `barrier=guard_rail` | Glissière sécurité | 🔴🔴 Prévention sortie route |
| `traffic_signals` | Feu tricolore | ⭐ Carrefour dangereux |
| `amenity=fuel` | Station essence | ⭐ Repos conducteur |
| `highway=traffic_signals` | Panneau stop | ⭐ Intersection |

### c) Caractéristiques routes (contexte)

```
[out:json];
way["highway"~"motorway|trunk|primary|secondary"](bbox);
out geom;
```

**Critères de sélection** :
✅ Données publiques (OpenStreetMap community)
✅ Géolocalisées précisément (lat/lon)
✅ Mises à jour fréquentes (2-3 fois/an)
✅ Correspond à variables BAAC "route_type"
⚠️ Limitation : OSM dépend de contributions bénévoles (complétude inégale par région)

**Stratégie de requête** :

- Pour chaque accident BAAC : query Overpass rayon **500m** autour GPS
- Compte nombre d'éléments (ex: "5 radars à 500m" vs "0 radar")
- Agréger par type d'accident (grave vs léger) → corrélation

---

### 4. **Données Véhicules & Conducteurs - BAAC (Usagers)**

**Intégration** : Déjà dans BAAC (fichier "Usagers"), pas API externe

**Dimensions analysables** :

- **Âge conducteur** : 16-90 ans (jeunes conducteurs < 25 = +30% accidents graves)
- **Type véhicule** : Voiture/Moto/Camion/Cycliste
- **Gravité blessure** : Permet modélisation "risque de décès"

**Intérêt** :
✅ Lier accident → profil conducteur → type véhicule → gravité
✅ Exemple rapport : "Conducteurs 18-25 ans de nuit en moto sur route secondaire sans glissière = 8x plus mortel"

---

### 5. **Données Administratives (Bonus, optionnel Phase 2)**

**Données en attente** :

- **INSEE démographie** : Population par commune (pour taux accident/habitant)
- **Cadastre** : Type territoire (rural/urbain/périurbain)
- **Budget routes** : Investissement régional sécurité routière

**Impact** : Permet corrélations socio-économiques (communes pauvres = moins d'investissement = plus d'accidents)

---

## Architecture technique du système d'ingestion

### Workflows du Bot

```
┌─────────────────────────────────────────────────────────────┐
│                      BOT PYTHON (BOTME)                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Phase 1] TÉLÉCHARGEMENT BAAC                              │
│  ├─ data.gouv.fr API : requête /datasets/...                │
│  ├─ Télécharge CSV années (2018-2023)                       │
│  └─ Stockage local : /data/baac/accidents_*.csv             │
│                                                             │
│  [Phase 2] PARSING & VALIDATION                             │
│  ├─ Pandas read_csv() with encoding latin-1                 │
│  ├─ Validation colonnes clefs (lat/lon/heure/gravite)       │
│  ├─ Filtrage doublons (num_acc unique)                      │
│  └─ Join Caractéristiques + Lieux + Usagers                 │
│                                                             │
│  [Phase 3] ENRICHISSEMENT MÉTÉO (Open-Meteo API)            │
│  ├─ Pour chaque accident:                                   │
│  │   ├─ GET /archive?lat={}&lon={}&date={}                  │
│  │   ├─ Match heure exacte de l'accident                    │
│  │   └─ Ajoute: temp, pluie, vent, visibilité               │
│  ├─ Rate limiting: 1 requête/500ms (120 req/min)            │
│  └─ Cache local (évite re-requêtes)                         │
│                                                             │
│  [Phase 4] ENRICHISSEMENT INFRASTRUCTURES (Overpass API)    │
│  ├─ Pour chaque accident (tous les N%)                      │
│  │   ├─ Query Overpass: radars/glissières rayon 500m        │
│  │   ├─ Parse JSON response                                 │
│  │   └─ Agrège counts (nb_radars, nb_guard_rails)           │
│  ├─ Rate limiting: 1 requête/2s (30 req/min)                │
│  └─ Cache géographique (même bbox = même réponse)           │
│                                                             │
│  [Phase 5] CALCUL INDICATEURS DÉRIVÉS                       │
│  ├─ nuit = (heure >= 20 OR heure <= 6)                      │
│  ├─ conditions_severes = (pluie > 5mm OR vent > 40)         │
│  ├─ infra_complete = (nb_radars >= 1 AND nb_guard_rails >= 1) │
│  ├─ risk_profile = f(age, type_vehicle, heure, route_type)  │
│  └─ gravite_binaire = (1 si mort/grave, 0 sinon)            │
│                                                             │
│  [Phase 6] STRUCTURATION JSON & SÉRIALIZATION               │
│  └─ Format:                                                 │
│     {                                                       │
│       "id_unique": "202300001234",                          │
│       "timestamp": "2023-06-15T02:30:00",                   │
│       "coordonnees": {"lat": 48.8566, "lon": 2.3522},       │
│       "accident": {                                         │
│         "gravite": 4,  /* 1-4 scale */                      │
│         "type_collision": "frontal",                        │
│         "route_type": "autoroute"                           │
│       },                                                    │
│       "contexte_temps": {                                   │
│         "jour_semaine": 5,  /* 1=lun, 7=dim */              │
│         "heure": 2,  /* 0-23 */                             │
│         "est_nuit": true,  /* heure >= 20 ou <= 6 */        │
│         "mois": 6,  /* saisonnalité */                      │
│         "luminosite": 3  /* 1=jour, 2=crépuscule, 3=nuit */ │
│       },                                                    │
│       "meteo": {                                            │
│         "temperature_c": 18.5,                              │
│         "precipitation_mm": 12.4,                           │
│         "windspeed_kmh": 35.2,                              │
│         "visibility_m": 800,  /* brouillard */              │
│         "humidity_pct": 92,                                 │
│         "conditions_severes": true,  /* dérivé */           │
│         "code_meteo": 61  /* WMO code: 61 = rainy */        │
│       },                                                    │
│       "infrastructures": {                                  │
│         "radars_500m": 2,  /* count Overpass */             │
│         "glissieres_500m": 1,                               │
│         "feux_tricolores_500m": 0,                          │
│         "infrastructure_adequate": false  /* dérivé */      │
│       },                                                    │
│       "conducteur_principal": {                             │
│         "age": 28,  /* from BAAC usagers */                 │
│         "sexe": 1,  /* 1=M, 2=F */                          │
│         "type_usager": "conducteur",                        │
│         "gravite": 4  /* 1-4, 4=décès */                    │
│       },                                                    │
│       "caracteristiques_route": {                           │
│         "largeur_chaussee_m": 7.5,                          │
│         "rayon_courbure_m": 150,  /* petit = courbe */      │
│         "pente_pct": 8,  /* montée/descente */              │
│         "agglomeration": false,                             │
│         "type_intersection": "sans"                         │
│       },                                                    │
│       "indicateurs_risque": {                               │
│         "age_risque": true,  /* age < 25 */                 │
│         "nuit_risque": true,  /* heure >= 20 */             │
│         "meteo_risque": true,  /* pluie + vent */           │
│         "infra_risque": true  /* pas de radars */           │
│       }                                                     │
│     }                                                       │
│                                                             │
│  [Phase 7] INJECTION VERS LOGSTASH                          │
│  ├─ Socket TCP: localhost:5000                              │
│  ├─ Batching: 500 documents/push                            │
│  ├─ Format: Newline-delimited JSON (NDJSON)                 │
│  └─ Logging: /logs/injection_YYYY-MM-DD.log                 │
│                                                             │
│  [Phase 8] SCHEDULING (APScheduler)                         │
│  ├─ Run complet: 1x/mois (après ONISR publie nouvelle année)│
│  ├─ Mise à jour météo: Daily (rétrospective jours -30)      │
│  └─ Uptime monitoring: log succès/erreurs                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
        │                                    │
        │ NDJSON JSON docs                   │ Logstash consume
        ▼                                    ▼
┌─────────────────────────────────────────────────────────────┐
│              LOGSTASH PIPELINE (conf/logstash.conf)         │
├─────────────────────────────────────────────────────────────┤
│ input { tcp { port => 5000 codec => "json_lines" } }        │
│ filter {                                                    │
│   mutate { convert => { "gravite" => "integer" } }          │
│   date { match => ["timestamp", "ISO8601"] }                │
│ }                                                           │
│ output { elasticsearch { hosts => ["localhost:9200"] } }    │
└─────────────────────────────────────────────────────────────┘
        │
        │ Index: "accidents-YYYY.MM.DD"
        ▼
┌─────────────────────────────────────────────────────────────┐
│             ELASTICSEARCH (indices + mappings)              │
├─────────────────────────────────────────────────────────────┤
│ Index name: "accidents-*" (rollover monthly)                │
│ Shards: 3, Replicas: 1                                      │
│ Mappings:                                                   │
│  - id_unique: keyword (unique ID)                           │
│  - @timestamp: date (index standard)                        │
│  - coordonnees: geo_point (pour cartes)                     │
│  - heure: byte (aggregations)                               │
│  - gravite: byte (1-4)                                      │
│  - precipitation_mm: float (analytics)                      │
│  - est_nuit: boolean (filter facet)                         │
│  - conditions_severes: boolean (dashboards)                 │
│  - meteo.temperature_c: float (range filter)                │
└─────────────────────────────────────────────────────────────┘
        │
        │ Real-time indices
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    KIBANA (VISUALISATIONS)                  │
├─────────────────────────────────────────────────────────────┤
│ Dashboards:                                                 │
│  1. Heatmap France accidents mortels                        │
│  2. Time series gravité vs conditions météo                 │
│  3. Corrélations infrastructure vs accidents                │
│  4. Profils conducteurs risque (âge, sexe, véhicule)        │
│  5. Graphe relationnel (nuit + pluie + pas radar = mortel)  │
└─────────────────────────────────────────────────────────────┘

```

## Structure fichiers (Arborescence attendue)

#TODO

---


## Références / sources

#TODO

- BAAC [data.gouv.fr](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024) Bases de données annuelles des accidents corporels de la circulation routière - Années de 2005 à 2024
- Open-Meteo Historical Weather API documentation
- Open-Meteo Python package (RDocumentation)
- Open-Meteo Historical Forecast API
- Overpass API Wiki documentation
- Overpass Turbo OSINT tutorial (Hackers Arise)

---

## Notes complémentaires

### Choix technologiques justifiés

1. **Open-Meteo vs Météo-France API** : Gratuit, pas de quota, historique 1940-présent
2. **Overpass vs WMS direct** : Plus flexible pour requêtes custom, meilleure couverture France
3. **Pandas vs Spark** : Volume < 1M rows = Pandas suffisant, moins d'infrastructure
4. **ELK vs PostgreSQL** : Time series native, visualisations Kibana nativesburger, historique logs

### Variables manquantes / sources externes futures

- **Trafic routier** (via Google Maps API payante)
- **Alcoolémie conducteurs** (données ONISR brutes, confidentielles)
- **Investissements régionaux routes** (budget publics régionaux)

### Format données final (JSON exemple)

Voir section "Architecture technique" → Phase 6 pour exemple complet structuré