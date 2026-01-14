# DM12 - BOT BAAC ETL & Enrichment Pipeline

## SYNOPSIS

This utility performs extraction, transformation, and loading (ETL) of French road accident data (BAAC). It normalizes raw CSV files, enriches geographical coordinates with infrastructure data using the Overpass API (OpenStreetMap), and indexes the processed datasets into an Elasticsearch cluster.

## REQUIREMENTS

*   Python 3.8+
*   Network access to an Overpass API instance (public or local)
*   Network access to an Elasticsearch instance (if `--send-elk` is used)

## INSTALLATION

1.  Create a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## DATA PREPARATION

The script expects raw CSV files to be organized by year. You must download the annual datasets from [data.gouv.fr - Bases de données annuelles des accidents corporels de la circulation routière](https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022/).

The directory structure must follow this format inside your `data/raw` directory:

```text
data/raw/
├── 2022/
│   ├── caracteristiques-2022.csv
│   ├── lieux-2022.csv
│   ├── usagers-2022.csv
│   └── vehicules-2022.csv
├── 2023/
│   ├── ...
└── ...
```

*Note: The script automatically detects files based on keywords (caract, lieux, usagers, vehicules) regardless of the exact filename prefix.*

## USAGE

```bash
python3 src/main.py [OPTIONS]
```

### OPTIONS

**General Configuration**

*   `--data-dir PATH`
    Directory containing the raw CSV files organized by year (default: `data/raw`).

*   `--cache-dir PATH`
    Directory for storing intermediate serialized data (default: `data/cache`).

*   `--n-jobs INT`
    Number of parallel jobs for file processing (default: 10).

*   `--sample-size INT`
    Process only a random sample of N accidents (useful for testing).

**Elasticsearch Configuration**

*   `--send-elk`
    Enable data indexing to Elasticsearch.

*   `--elk-host HOST`
    Elasticsearch hostname (default: `localhost`).

*   `--elk-port PORT`
    Elasticsearch port (default: `9200`).

*   `--elk-user USER`
    Username for Elasticsearch authentication (can also use env var `ELK_USER`).

*   `--elk-password PASS`
    Password for Elasticsearch authentication (can also use env var `ELK_PASSWORD`).

**Enrichment Configuration**

*   `--skip-overpass`
    Disable OpenStreetMap infrastructure enrichment.

*   `--overpass-url URL`
    API endpoint for Overpass queries (default: `http://localhost:12345/api/interpreter`).

*   `--enrich-only`
    Skip the import phase and only perform enrichment on existing Elasticsearch data.

## EXAMPLES

**1. Full Import**
Import all available years, enrich data via Overpass, and index to a local Elasticsearch instance.

```bash
python3 src/main.py --send-elk --n-jobs 12
```

**2. Dry Run**
Process data and create the local cache without indexing to Elasticsearch. Useful to validate data integrity.

```bash
python3 src/main.py --data-dir ./data/raw
```

**3. Test on Sample**
Process a random sample of 1000 accidents to validate the pipeline.

```bash
python3 src/main.py --sample-size 1000 --send-elk
```

## OUTPUT DATA MODEL

The pipeline generates four distinct indices in Elasticsearch to handle the one-to-many relationships inherent in the BAAC schema:

1.  `accidents-routiers`: Main accident characteristics and enriched infrastructure data.
2.  `accidents-lieux`: Location details (one accident may have multiple locations).
3.  `accidents-vehicules`: Vehicles involved.
4.  `accidents-usagers`: People involved (drivers, passengers, pedestrians).

Links between indices are maintained via the `num_acc` field.