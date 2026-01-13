import pandas as pd
import os
import glob
import time
from joblib import Parallel, delayed, dump, load, hash as joblib_hash
import logging
from charset_normalizer import from_path

logger = logging.getLogger("DM12")

class BAACLoader:
    def __init__(self, data_dir="data/raw", cache_dir="data/cache"):
        self.data_dir = data_dir
        self.cache_dir = cache_dir
        self.cache_file = os.path.join(cache_dir, "baac_all_years_full.pkl")
        os.makedirs(cache_dir, exist_ok=True)

    def _detect_encoding(self, filepath):
        """Détecte l'encodage d'un fichier avec charset-normalizer"""
        result = from_path(filepath).best()
        if result is None:
            return 'utf-8'
        return result.encoding

    def _get_data_signature(self):
        """Génère une signature unique basée sur les fichiers présents et leur taille"""
        year_dirs = glob.glob(os.path.join(self.data_dir, "[12][0-9][0-9][0-9]"))
        years = sorted([int(os.path.basename(d)) for d in year_dirs])

        signature = []
        for year in years:
            base_path = os.path.join(self.data_dir, str(year))
            for keyword in ['caract', 'lieux', 'vehicules', 'usagers']:
                try:
                    files = glob.glob(os.path.join(base_path, f"*{keyword}*.csv"))
                    if files:
                        fsize = os.path.getsize(files[0])
                        signature.append(f"{year}_{keyword}_{fsize}")
                except:
                    pass

        return joblib_hash("_".join(signature))

    def _normalize_columns(self, df):
        """Normalise les noms de colonnes (lowercase + mapping standard)"""
        df.columns = df.columns.str.lower()
        
        column_mapping = {
            'accident_id': 'num_acc',
            'agglo': 'agg',
            'id_vehicule': 'id_vehicule',
            'num_veh': 'num_veh',
        }
        
        rename_dict = {old: new for old, new in column_mapping.items()
                      if old in df.columns and old != new}
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)
        
        return df

    def _clean_numeric_codes(self, df):
        """Nettoie les codes numériques pour éviter les valeurs invalides"""
        # Convertir num_acc en string
        if 'num_acc' in df.columns:
            df['num_acc'] = df['num_acc'].astype(str)
        
        # Nettoyer dep et com
        if 'dep' in df.columns:
            df['dep'] = df['dep'].astype(str).str.rstrip('0').str.zfill(2)
        if 'com' in df.columns:
            df['com'] = df['com'].astype(str).str.replace('.0', '', regex=False).str.zfill(3)
        
        return df

    def _process_timestamp(self, df, year):
        """Crée un timestamp propre à partir des colonnes temporelles"""
        if 'hrmn' in df.columns:
            df['hrmn'] = df['hrmn'].fillna('0000').astype(str).str.replace(':', '').str.zfill(4)
            mask_valid_hrmn = df['hrmn'].str.match(r'^([0-1]?[0-9]|2[0-3])[0-5][0-9]$', na=False)
            df.loc[~mask_valid_hrmn, 'hrmn'] = '0000'
            df['heure'] = df['hrmn'].str[:2].astype(int)
            df['minute'] = df['hrmn'].str[2:].astype(int)
        else:
            df['heure'] = 0
            df['minute'] = 0

        if 'an' in df.columns:
            df['an'] = df['an'].apply(lambda x: x + 2000 if x < 100 else x)
        else:
            df['an'] = year

        if all(c in df.columns for c in ['an', 'mois', 'jour', 'heure', 'minute']):
            df['timestamp'] = pd.to_datetime(
                df['an'].astype(str) + '-' +
                df['mois'].fillna(1).astype(int).astype(str).str.zfill(2) + '-' +
                df['jour'].fillna(1).astype(int).astype(str).str.zfill(2) + ' ' +
                df['heure'].astype(str).str.zfill(2) + ':' +
                df['minute'].astype(str).str.zfill(2),
                errors='coerce'
            )
        else:
            df['timestamp'] = pd.NaT

        return df

    def _process_coordinates(self, df):
        """Nettoie et valide les coordonnées GPS"""
        if 'lat' in df.columns:
            df['lat'] = df['lat'].astype(str).str.replace(',', '.')
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        else:
            df['lat'] = None

        if 'long' in df.columns:
            df['long'] = df['long'].astype(str).str.replace(',', '.')
            df['long'] = pd.to_numeric(df['long'], errors='coerce')
        else:
            df['long'] = None

        if 'lat' in df.columns and 'long' in df.columns:
            mask_aberrant = (
                ((df['lat'] == 0) & (df['long'] == 0)) |
                (df['lat'].abs() > 90) |
                (df['long'].abs() > 180)
            )
            df.loc[mask_aberrant, ['lat', 'long']] = None

        return df

    def load_year(self, year):
        """
        Charge les 4 fichiers pour une année donnée et retourne un dict structuré.
        CONSERVE TOUTES LES COLONNES sans agrégation destructive.
        """
        base_path = os.path.join(self.data_dir, str(year))
        
        try:
            # 1. LECTURE DES 4 FICHIERS
            carac_file = self._find_file(base_path, "caract")
            lieux_file = self._find_file(base_path, "lieux")
            veh_file = self._find_file(base_path, "vehicules")
            usagers_file = self._find_file(base_path, "usagers")

            df_carac = pd.read_csv(
                carac_file, sep=None, engine='python',
                encoding=self._detect_encoding(carac_file),
                on_bad_lines='skip'
            )
            df_lieux = pd.read_csv(
                lieux_file, sep=None, engine='python',
                encoding=self._detect_encoding(lieux_file),
                on_bad_lines='skip'
            )
            df_veh = pd.read_csv(
                veh_file, sep=None, engine='python',
                encoding=self._detect_encoding(veh_file),
                on_bad_lines='skip'
            )
            df_usagers = pd.read_csv(
                usagers_file, sep=None, engine='python',
                encoding=self._detect_encoding(usagers_file),
                on_bad_lines='skip'
            )

            # 2. NORMALISATION DES COLONNES
            df_carac = self._normalize_columns(df_carac)
            df_lieux = self._normalize_columns(df_lieux)
            df_veh = self._normalize_columns(df_veh)
            df_usagers = self._normalize_columns(df_usagers)

            # 3. NETTOYAGE CODES
            for df in [df_carac, df_lieux, df_veh, df_usagers]:
                self._clean_numeric_codes(df)

            # 4. TRAITEMENT TIMESTAMP ET GPS (uniquement sur carac)
            df_carac = self._process_timestamp(df_carac, year)
            df_carac = self._process_coordinates(df_carac)

            # 5. FUSION CARACTERISTIQUES + LIEUX (1:1)
            df_accident = pd.merge(df_carac, df_lieux, on='num_acc', how='left', suffixes=('', '_lieux'))

            # 6. STRUCTURE FINALE : 
            # - Un dict par accident avec TOUTES ses colonnes
            # - Une liste de véhicules avec TOUTES leurs colonnes
            # - Une liste d'usagers avec TOUTES leurs colonnes
            
            result = {
                'accidents': df_accident,
                'vehicules': df_veh,
                'usagers': df_usagers,
                'year': year
            }

            logger.info(f"✅ {year}: {len(df_accident)} accidents, {len(df_veh)} véhicules, {len(df_usagers)} usagers")
            return result

        except Exception as e:
            logger.error(f"❌ Erreur lecture {year} : {e}")
            return None

    def _find_file(self, path, keyword):
        files = glob.glob(os.path.join(path, f"*{keyword}*.csv"))
        if not files:
            files = glob.glob(os.path.join(path, f"*{keyword.capitalize()}*.csv"))
        if not files:
            raise FileNotFoundError(f"Fichier '{keyword}' introuvable dans {path}")
        return files[0]

    def load_all_years(self, n_jobs=10, force_reload=False):
        """
        Charge toutes les années et retourne un DataFrame structuré avec nested data.
        """
        current_signature = self._get_data_signature()
        cache_signature_file = self.cache_file + ".sig"

        # Check cache
        if not force_reload and os.path.exists(self.cache_file) and os.path.exists(cache_signature_file):
            with open(cache_signature_file, 'r') as f:
                cached_signature = f.read().strip()

            if cached_signature == current_signature:
                logger.info("📦 Cache BAAC complet trouvé, chargement rapide...")
                data = load(self.cache_file)
                logger.info(f"✅ {len(data):,} accidents chargés depuis le cache")
                return data
            else:
                logger.info("🔄 Cache obsolète, rechargement...")
        else:
            logger.info("📥 Pas de cache, chargement complet...")

        # Chargement parallèle
        year_dirs = glob.glob(os.path.join(self.data_dir, "[12][0-9][0-9][0-9]"))
        years = sorted([int(os.path.basename(d)) for d in year_dirs])

        if not years:
            raise FileNotFoundError(f"❌ Aucune année trouvée dans {self.data_dir}/")

        logger.info(f"📅 Années : {years}")
        logger.info(f"⚡ Chargement parallèle (n_jobs={n_jobs})")

        results = Parallel(n_jobs=n_jobs, verbose=10)(
            delayed(self.load_year)(year) for year in years
        )

        # Concaténation
        all_accidents = []
        all_vehicules = []
        all_usagers = []

        for r in results:
            if r is not None:
                all_accidents.append(r['accidents'])
                all_vehicules.append(r['vehicules'])
                all_usagers.append(r['usagers'])

        df_accidents = pd.concat(all_accidents, ignore_index=True)
        df_vehicules = pd.concat(all_vehicules, ignore_index=True)
        df_usagers = pd.concat(all_usagers, ignore_index=True)

        logger.info(f"✅ TOTAL : {len(df_accidents):,} accidents, {len(df_vehicules):,} véhicules, {len(df_usagers):,} usagers")

        # Structuration finale : dict avec les 3 DataFrames
        data = {
            'accidents': df_accidents,
            'vehicules': df_vehicules,
            'usagers': df_usagers
        }

        # Sauvegarde cache
        logger.info(f"💾 Sauvegarde du cache...")
        dump(data, self.cache_file, compress=3)
        with open(cache_signature_file, 'w') as f:
            f.write(current_signature)
        logger.info("✅ Cache sauvegardé")

        return data