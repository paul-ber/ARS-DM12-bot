import pandas as pd
import os
import logging
from joblib import Memory, Parallel, delayed
from pathlib import Path

logger = logging.getLogger("DM12")

class BAACLoader:
    """Charge et nettoie les données BAAC avec cache joblib"""

    def __init__(self, data_dir="data/raw", cache_dir="data/cache"):
        self.data_dir = Path(data_dir)
        self.cache_dir = Path(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        self.memory = Memory(self.cache_dir, verbose=0)

    def _parse_gps_coordinate(self, value):
        """
        Parse les coordonnées GPS qui ont 2 formats selon l'année :
        - Ancien format (2005-2018) : "5051974" → 50.51974
        - Nouveau format (2019+) : "48,6978200" → 48.6978200
        """
        if pd.isna(value) or value == '':
            return None

        value_str = str(value).strip()

        # Format avec virgule (2019+)
        if ',' in value_str or '.' in value_str:
            try:
                return float(value_str.replace(',', '.'))
            except:
                return None

        # Format ancien sans virgule (2005-2018)
        try:
            num = float(value_str)
            # Si > 1000, c'est l'ancien format (ex: 5051974 → 50.51974)
            if abs(num) > 1000:
                return num / 100000.0
            return num
        except:
            return None

    def _clean_text_field(self, value):
        """Nettoie les champs texte"""
        if pd.isna(value) or value == '':
            return None
        return str(value).strip()

    def _load_and_clean_accidents(self, year):
        """Charge et nettoie les accidents d'une année"""
        files = list(self.data_dir.glob(f"{year}/*caracteristiques*{year}*.csv"))

        if not files:
            logger.warning(f"Fichier caractéristiques {year} manquant")
            return pd.DataFrame()

        file_path = files[0]

        try:
            df = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)

            # Renommer colonnes si besoin
            if 'Num_Acc' in df.columns:
                df = df.rename(columns={'Num_Acc': 'num_acc'})

            # Normaliser num_acc
            if 'num_acc' in df.columns:
                df['num_acc'] = df['num_acc'].astype(str)

            # Parser GPS (2 formats selon l'année)
            if 'lat' in df.columns:
                df['lat'] = df['lat'].apply(self._parse_gps_coordinate)
            if 'long' in df.columns:
                df['long'] = df['long'].apply(self._parse_gps_coordinate)

            # Nettoyer nbv : remplacer #VALEURMULTI par None
            if 'nbv' in df.columns:
                df['nbv'] = df['nbv'].apply(lambda x: None if str(x).strip() == '#VALEURMULTI' else x)
                df['nbv'] = pd.to_numeric(df['nbv'], errors='coerce')

            # Nettoyer champs texte/numériques gardés en texte dans ELK
            text_numeric_fields = ['larrout', 'lartpc', 'pr', 'pr1']
            for field in text_numeric_fields:
                if field in df.columns:
                    df[field] = df[field].apply(lambda x: str(x).strip().replace(' ', '').replace(',', '.') if pd.notna(x) and str(x).strip() != '' else None)
                    df[field] = df[field].apply(lambda x: None if x in ['(1)', '-1', 'nan', 'None'] else x)

            # Nettoyer champs texte
            text_fields = ['voie', 'v1', 'v2', 'adr']
            for field in text_fields:
                if field in df.columns:
                    df[field] = df[field].apply(self._clean_text_field)

            # Créer timestamp avec fuseau Europe/Paris
            if all(col in df.columns for col in ['an', 'mois', 'jour']):
                df['heure'] = df.get('heure', 0).fillna(0).astype(int)
                df['minute'] = df.get('minute', 0).fillna(0).astype(int)

                df['timestamp'] = pd.to_datetime(
                    df[['an', 'mois', 'jour', 'heure', 'minute']].rename(
                        columns={'an': 'year', 'mois': 'month', 'jour': 'day',
                                'heure': 'hour', 'minute': 'minute'}
                    ),
                    errors='coerce'
                ).dt.tz_localize('Europe/Paris', ambiguous='NaT', nonexistent='NaT')

            logger.info(f"✅ {year}: {len(df):,} accidents chargés et nettoyés")
            return df

        except Exception as e:
            logger.error(f"Erreur lors du chargement de {year}: {e}")
            return pd.DataFrame()

    def _load_and_clean_vehicules(self, year):
        """Charge et nettoie les véhicules d'une année"""
        files = list(self.data_dir.glob(f"{year}/*vehicule*{year}*.csv"))

        if not files:
            logger.warning(f"Fichier véhicules {year} manquant")
            return pd.DataFrame()

        file_path = files[0]

        try:
            df = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)

            if 'Num_Acc' in df.columns:
                df = df.rename(columns={'Num_Acc': 'num_acc'})

            if 'num_acc' in df.columns:
                df['num_acc'] = df['num_acc'].astype(str)

            logger.debug(f"{year}: {len(df):,} véhicules")
            return df

        except Exception as e:
            logger.error(f"Erreur véhicules {year}: {e}")
            return pd.DataFrame()

    def _load_and_clean_usagers(self, year):
        """Charge et nettoie les usagers d'une année"""
        files = list(self.data_dir.glob(f"{year}/*usager*{year}*.csv"))

        if not files:
            logger.warning(f"Fichier usagers {year} manquant")
            return pd.DataFrame()

        file_path = files[0]

        try:
            df = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)

            if 'Num_Acc' in df.columns:
                df = df.rename(columns={'Num_Acc': 'num_acc'})

            if 'num_acc' in df.columns:
                df['num_acc'] = df['num_acc'].astype(str)

            # Calculer l'âge si an_nais existe
            if 'an_nais' in df.columns:
                df['an_nais'] = pd.to_numeric(df['an_nais'], errors='coerce')
                df['age'] = 2026 - df['an_nais']
                df.loc[df['an_nais'] < 1900, 'age'] = None
                df.loc[df['age'] < 0, 'age'] = None
                df.loc[df['age'] > 120, 'age'] = None

            logger.debug(f"{year}: {len(df):,} usagers")
            return df

        except Exception as e:
            logger.error(f"Erreur usagers {year}: {e}")
            return pd.DataFrame()

    def load_all_years(self, years=None, n_jobs=10, force_reload=False):
        """
        Charge toutes les années en parallèle avec cache joblib

        Args:
            years: Liste d'années (None = 2005-2024)
            n_jobs: Nombre de workers parallèles
            force_reload: Force le rechargement (ignore le cache)
        """
        if years is None:
            years = list(range(2005, 2025))

        if force_reload:
            self.memory.clear(warn=False)
            logger.info("Cache joblib vidé")

        # Wrapper pour cache
        load_acc_cached = self.memory.cache(self._load_and_clean_accidents)
        load_veh_cached = self.memory.cache(self._load_and_clean_vehicules)
        load_usr_cached = self.memory.cache(self._load_and_clean_usagers)

        logger.info(f"Chargement de {len(years)} années ({n_jobs} workers)...")

        # Chargement parallèle
        accidents_list = Parallel(n_jobs=n_jobs, backend='loky')(
            delayed(load_acc_cached)(year) for year in years
        )

        vehicules_list = Parallel(n_jobs=n_jobs, backend='loky')(
            delayed(load_veh_cached)(year) for year in years
        )

        usagers_list = Parallel(n_jobs=n_jobs, backend='loky')(
            delayed(load_usr_cached)(year) for year in years
        )

        # Concaténation
        df_accidents = pd.concat([df for df in accidents_list if not df.empty], ignore_index=True)
        df_vehicules = pd.concat([df for df in vehicules_list if not df.empty], ignore_index=True)
        df_usagers = pd.concat([df for df in usagers_list if not df.empty], ignore_index=True)

        logger.info(f"Total chargé : {len(df_accidents):,} accidents, "
                   f"{len(df_vehicules):,} véhicules, {len(df_usagers):,} usagers")

        return {
            'accidents': df_accidents,
            'vehicules': df_vehicules,
            'usagers': df_usagers
        }
