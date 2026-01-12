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
        self.cache_file = os.path.join(cache_dir, "baac_all_years.pkl")
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

        # Signature = hash(années + tailles des fichiers CSV)
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

    def load_year(self, year):
        """Charge et fusionne les 4 fichiers pour une année donnée"""
        base_path = os.path.join(self.data_dir, str(year))
        t_start = time.time()

        # 1. Lecture des fichiers
        try:
            carac_file = self._find_file(base_path, "caract")
            lieux_file = self._find_file(base_path, "lieux")
            veh_file = self._find_file(base_path, "vehicules")
            usagers_file = self._find_file(base_path, "usagers")

            df_carac = pd.read_csv(
                carac_file,
                sep=None,
                engine='python',
                encoding=self._detect_encoding(carac_file),
                on_bad_lines='skip'
            )

            df_lieux = pd.read_csv(
                lieux_file,
                sep=None,
                engine='python',
                encoding=self._detect_encoding(lieux_file),
                on_bad_lines='skip'
            )

            df_veh = pd.read_csv(
                veh_file,
                sep=None,
                engine='python',
                encoding=self._detect_encoding(veh_file),
                on_bad_lines='skip'
            )

            df_usagers = pd.read_csv(
                usagers_file,
                sep=None,
                engine='python',
                encoding=self._detect_encoding(usagers_file),
                on_bad_lines='skip'
            )

        except Exception as e:
            logger.error(f"Erreur lecture {year} : {e}")
            return None

        # 2. Normalisation & mapping colonnes
        column_mapping = {
            'accident_id': 'num_acc',
            'num_acc': 'num_acc',
            'grav': 'grav',
            'an_nais': 'an_nais',
            'catv': 'catv',
            'an': 'an',
            'mois': 'mois',
            'jour': 'jour',
            'hrmn': 'hrmn',
            'lat': 'lat',
            'long': 'long',
            'dep': 'dep',
            'com': 'com',
            'catr': 'catr',
            'lum': 'lum',
            'agg': 'agg',
            'agglo': 'agg',
        }

        for df in [df_carac, df_lieux, df_veh, df_usagers]:
            df.columns = df.columns.str.lower()
            rename_dict = {old: new for old, new in column_mapping.items()
                          if old in df.columns and old != new}
            if rename_dict:
                df.rename(columns=rename_dict, inplace=True)

            if 'num_acc' in df.columns:
                df['num_acc'] = df['num_acc'].astype(str)
            if 'dep' in df.columns:
                df['dep'] = df['dep'].astype(str).str.rstrip('0').str.zfill(2)
            if 'com' in df.columns:
                df['com'] = df['com'].astype(str).str.replace('.0', '', regex=False).str.zfill(3)

        # 3. Agrégation Usagers
        if 'grav' in df_usagers.columns:
            usagers_agg = df_usagers.groupby('num_acc').agg(
                nb_tues=('grav', lambda x: (x == 2).sum() if len(x) > 0 else 0),
                nb_graves=('grav', lambda x: (x == 3).sum() if len(x) > 0 else 0),
                gravite_max=('grav', lambda x: x.min() if len(x) > 0 else 4)
            ).reset_index()

            usagers_agg['gravite_accident'] = usagers_agg['gravite_max'].apply(
                lambda x: "Mortel" if x == 2 else ("Grave" if x == 3 else "Léger")
            )
            usagers_agg.drop('gravite_max', axis=1, inplace=True)
        else:
            usagers_agg = df_usagers[['num_acc']].drop_duplicates()
            usagers_agg['nb_tues'] = 0
            usagers_agg['nb_graves'] = 0
            usagers_agg['gravite_accident'] = 'Inconnu'

        # 4. Agrégation Véhicules
        if 'catv' in df_veh.columns:
            def check_catv(group, codes):
                return group.isin(codes).any() if len(group) > 0 else False

            veh_agg = df_veh.groupby('num_acc').agg(
                implique_moto=('catv', lambda x: check_catv(x, [30,31,32,33,34,1,2])),
                implique_pl=('catv', lambda x: check_catv(x, [13,14,15,16,17])),
                implique_velo=('catv', lambda x: check_catv(x, [1])),
                nb_vehicules=('catv', 'count')
            ).reset_index()
        else:
            veh_agg = df_veh[['num_acc']].drop_duplicates()
            veh_agg['implique_moto'] = False
            veh_agg['implique_pl'] = False
            veh_agg['implique_velo'] = False
            veh_agg['nb_vehicules'] = 0

        # 5. Fusions
        df_final = pd.merge(df_carac, df_lieux, on='num_acc', how='left')
        df_final = pd.merge(df_final, usagers_agg, on='num_acc', how='left')
        df_final = pd.merge(df_final, veh_agg, on='num_acc', how='left')

        # 6. Timestamp
        if 'hrmn' in df_final.columns:
            df_final['hrmn'] = df_final['hrmn'].fillna('0000').astype(str).str.replace(':', '').str.zfill(4)
            mask_valid_hrmn = df_final['hrmn'].str.match(r'^([0-1]?[0-9]|2[0-3])[0-5][0-9]$', na=False)
            df_final.loc[~mask_valid_hrmn, 'hrmn'] = '0000'
            df_final['heure'] = df_final['hrmn'].str[:2].astype(int)
            df_final['minute'] = df_final['hrmn'].str[2:].astype(int)
        else:
            df_final['heure'] = 0
            df_final['minute'] = 0

        if 'an' in df_final.columns:
            df_final['an'] = df_final['an'].apply(lambda x: x + 2000 if x < 100 else x)
        else:
            df_final['an'] = year

        if all(c in df_final.columns for c in ['an', 'mois', 'jour', 'heure', 'minute']):
            df_final['timestamp'] = pd.to_datetime(
                df_final['an'].astype(str) + '-' +
                df_final['mois'].fillna(1).astype(int).astype(str).str.zfill(2) + '-' +
                df_final['jour'].fillna(1).astype(int).astype(str).str.zfill(2) + ' ' +
                df_final['heure'].astype(str).str.zfill(2) + ':' +
                df_final['minute'].astype(str).str.zfill(2),
                errors='coerce'
            )
        else:
            df_final['timestamp'] = pd.NaT

        # 7. Coordonnées
        if 'lat' in df_final.columns:
            df_final['lat'] = df_final['lat'].astype(str).str.replace(',', '.')
            df_final['lat'] = pd.to_numeric(df_final['lat'], errors='coerce')
        else:
            df_final['lat'] = None

        if 'long' in df_final.columns:
            df_final['long'] = df_final['long'].astype(str).str.replace(',', '.')
            df_final['long'] = pd.to_numeric(df_final['long'], errors='coerce')
        else:
            df_final['long'] = None

        if 'lat' in df_final.columns and 'long' in df_final.columns:
            mask_aberrant = (
                ((df_final['lat'] == 0) & (df_final['long'] == 0)) |
                (df_final['lat'].abs() > 90) |
                (df_final['long'].abs() > 180)
            )
            df_final.loc[mask_aberrant, ['lat', 'long']] = None

        return df_final

    def _find_file(self, path, keyword):
        files = glob.glob(os.path.join(path, f"*{keyword}*.csv"))
        if not files:
            files = glob.glob(os.path.join(path, f"*{keyword.capitalize()}*.csv"))
        if not files:
            raise FileNotFoundError(f"Fichier '{keyword}' introuvable dans {path}")
        return files[0]

    def load_all_years(self, n_jobs=10, force_reload=False):
        """
        Charge toutes les années avec cache automatique transparent.
        Le cache est invalidé si les fichiers sources changent.

        force_reload: Force le rechargement même si cache valide
        """
        # 1. Calcul signature des données
        current_signature = self._get_data_signature()
        cache_signature_file = self.cache_file + ".sig"

        # 2. Check cache
        if not force_reload and os.path.exists(self.cache_file) and os.path.exists(cache_signature_file):
            with open(cache_signature_file, 'r') as f:
                cached_signature = f.read().strip()

            if cached_signature == current_signature:
                logger.info("📦 Cache BAAC valide trouvé, chargement rapide...")
                df = load(self.cache_file)
                logger.info(f"✅ {len(df):,} accidents chargés depuis le cache")
                return df
            else:
                logger.info("🔄 Cache BAAC obsolète (données modifiées), rechargement...")
        else:
            logger.info("📥 Pas de cache BAAC, chargement complet...")

        # 3. Chargement complet
        year_dirs = glob.glob(os.path.join(self.data_dir, "[12][0-9][0-9][0-9]"))
        years = sorted([int(os.path.basename(d)) for d in year_dirs])

        if not years:
            raise FileNotFoundError(f"❌ Aucune année trouvée dans {self.data_dir}/")

        logger.info(f"📅 Années détectées : {years}")
        logger.info(f"⚡ Chargement parallèle (n_jobs={n_jobs})")

        results = Parallel(n_jobs=n_jobs, verbose=10)(
            delayed(self.load_year)(year) for year in years
        )

        all_dataframes = [df for df in results if df is not None and len(df) > 0]

        if not all_dataframes:
            raise ValueError("❌ Aucune donnée chargée !")

        logger.info("🔗 Concaténation finale...")
        df_combined = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"✅ TOTAL : {len(df_combined):,} accidents sur {len(all_dataframes)} années")

        # 4. Sauvegarde cache
        logger.info(f"💾 Sauvegarde du cache BAAC...")
        dump(df_combined, self.cache_file, compress=3)
        with open(cache_signature_file, 'w') as f:
            f.write(current_signature)
        logger.info("✅ Cache sauvegardé")

        return df_combined
