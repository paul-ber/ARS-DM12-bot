import os
import glob
import logging
import numpy as np
import pandas as pd
from charset_normalizer import from_path
from joblib import Parallel, delayed, dump, load, hash as joblibhash

logger = logging.getLogger("DM12")

class BAACLoader:
    def __init__(self, data_dir="data/raw", cache_dir="data/cache"):
        self.data_dir = data_dir
        self.cache_dir = cache_dir
        self.cache_file = os.path.join(cache_dir, "baac_all_years_full.pkl")
        os.makedirs(cache_dir, exist_ok=True)

    def detect_encoding(self, file_path):
        """Détecte l'encodage d'un fichier avec charset-normalizer"""
        result = from_path(file_path).best()
        if result is None:
            return "utf-8"
        return result.encoding

    def get_data_signature(self):
        """Génère une signature unique basée sur les fichiers présents et leur taille"""
        year_dirs = glob.glob(os.path.join(self.data_dir, "[12]0[0-9][0-9]"))
        years = sorted([int(os.path.basename(d)) for d in year_dirs])
        signature = []

        for year in years:
            base_path = os.path.join(self.data_dir, str(year))
            for keyword in ["caract", "lieux", "vehicules", "usagers"]:
                try:
                    files = glob.glob(os.path.join(base_path, f"*{keyword}*.csv"))
                    if files:
                        f_size = os.path.getsize(files[0])
                        signature.append(f"{year}-{keyword}-{f_size}")
                except:
                    pass

        return joblibhash("-".join(signature))

    def normalize_columns(self, df):
        """Normalise les noms de colonnes (lowercase + mapping standard)"""
        df.columns = df.columns.str.lower()

        column_mapping = {
            "accident_id": "num_acc",
            "agglo": "agg",
            "id_vehicule": "id_vehicule",
            "num_veh": "num_veh",
        }

        rename_dict = {old: new for old, new in column_mapping.items()
                      if old in df.columns and old != new}
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)

        return df

    def clean_numeric_codes(self, df):
        """Nettoie les codes numériques pour éviter les valeurs invalides

        Nettoie les champs suivants:
        - pr, pr1: supprime les parenthèses et espaces des nombres
        - larrout, lartpc: supprime les espaces et remplace virgule par point
        - nbv: remplace les valeurs non numériques par None
        - voie: convertit en string pour préserver les noms de routes
        """
        # Convertir num_acc en string
        if "num_acc" in df.columns:
            df["num_acc"] = df["num_acc"].astype(str)

        # Nettoyer pr (supprime parenthèses et espaces)
        if "pr" in df.columns:
            df["pr"] = df["pr"].astype(str).str.replace(r"[\(\)\s]", "", regex=True)
            df["pr"] = pd.to_numeric(df["pr"], errors="coerce")

        # Nettoyer pr1 (supprime parenthèses et espaces)
        if "pr1" in df.columns:
            df["pr1"] = df["pr1"].astype(str).str.replace(r"[\(\)\s]", "", regex=True)
            df["pr1"] = pd.to_numeric(df["pr1"], errors="coerce")

        # Nettoyer larrout (supprime espaces, remplace virgule par point)
        if "larrout" in df.columns:
            df["larrout"] = df["larrout"].astype(str).str.strip().str.replace(",", ".")
            df["larrout"] = pd.to_numeric(df["larrout"], errors="coerce")

        # Nettoyer lartpc (supprime espaces, remplace virgule par point)
        if "lartpc" in df.columns:
            df["lartpc"] = df["lartpc"].astype(str).str.strip().str.replace(",", ".")
            df["lartpc"] = pd.to_numeric(df["lartpc"], errors="coerce")

        # Nettoyer nbv (remplace valeurs non numériques)
        if "nbv" in df.columns:
            df["nbv"] = pd.to_numeric(df["nbv"].astype(str).replace("#VALEURMULTI", None), errors="coerce")

        # Nettoyer voie (convertir en string pour préserver les noms)
        if "voie" in df.columns:
            df["voie"] = df["voie"].astype(str)

        # Nettoyer dep et com
        if "dep" in df.columns:
            df["dep"] = df["dep"].astype(str).str.rstrip(".0").str.zfill(2)

        if "com" in df.columns:
            df["com"] = df["com"].astype(str).str.replace(".0", "", regex=False).str.zfill(3)

        return df

    def process_timestamp(self, df, year):
        """Crée un timestamp propre à partir des colonnes temporelles"""
        if "hrmn" in df.columns:
            df["hrmn"] = df["hrmn"].fillna("00:00").astype(str).str.replace(":", "").str.zfill(4)
            mask_valid_hrmn = df["hrmn"].str.match(r"[0-1]?[0-9][2-3][0-5][0-9]", na=False)
            df.loc[~mask_valid_hrmn, "hrmn"] = "0000"
            df["heure"] = df["hrmn"].str[:2].astype(int)
            df["minute"] = df["hrmn"].str[2:].astype(int)
        else:
            df["heure"] = 0
            df["minute"] = 0

        if "an" in df.columns:
            df["an"] = df["an"].apply(lambda x: x + 2000 if x < 100 else x)
        else:
            df["an"] = year

        if all(c in df.columns for c in ["an", "mois", "jour", "heure", "minute"]):
            df["timestamp"] = (
                pd.to_datetime(
                    df["an"].astype(str) + "-" +
                    df["mois"].fillna(1).astype(int).astype(str).str.zfill(2) + "-" +
                    df["jour"].fillna(1).astype(int).astype(str).str.zfill(2) + " " +
                    df["heure"].astype(str).str.zfill(2) + ":" +
                    df["minute"].astype(str).str.zfill(2),
                    errors="coerce"
                )
                .dt.tz_localize("Europe/Paris", ambiguous="NaT", nonexistent="NaT")
            )
        else:
            df["timestamp"] = pd.NaT

        return df

    def process_coordinates(self, df):
        """
        Nettoie et valide les coordonnées GPS avec support multi-format.
        VERSION DEBUG avec logs détaillés.
        """

        # LOG : Échantillon des données brutes AVANT traitement
        if "lat" in df.columns and "long" in df.columns:
            logger.info("=" * 80)
            logger.info("DEBUG GPS - Échantillon AVANT traitement:")
            sample = df[["num_acc", "lat", "long"]].head(10)
            for idx, row in sample.iterrows():
                logger.info(f"  {row['num_acc']} | lat={row['lat']!r} (type:{type(row['lat']).__name__}) | long={row['long']!r} (type:{type(row['long']).__name__})")

        def parse_gps_coordinate(value, coord_name="", num_acc=""):
            """Parse une coordonnée GPS selon son format."""
            if pd.isna(value):
                return np.nan

            # Convertir en string et nettoyer
            str_val = str(value).strip()

            # LOG: Valeur d'entrée
            logger.debug(f"[{num_acc}][{coord_name}] Input: {value!r} -> str: '{str_val}'")

            # Gérer les valeurs vides ou nulles explicites
            if not str_val or str_val.lower() in ['nan', 'none', '', '0']:
                logger.debug(f"[{num_acc}][{coord_name}] -> REJECTED (empty or zero)")
                return np.nan

            # Format moderne (2019+) : nombre avec séparateur décimal
            if ',' in str_val or '.' in str_val:
                str_val = str_val.replace(',', '.')
                try:
                    coord = float(str_val)
                    if coord == 0.0 or abs(coord) > 90:
                        logger.debug(f"[{num_acc}][{coord_name}] -> REJECTED modern format (zero or >90): {coord}")
                        return np.nan
                    logger.debug(f"[{num_acc}][{coord_name}] -> OK modern: {coord}")
                    return coord
                except ValueError:
                    logger.debug(f"[{num_acc}][{coord_name}] -> REJECTED (ValueError)")
                    return np.nan

            # Format ancien (2005-2018) : entier compacté DDMMMMM
            is_negative = str_val.startswith('-')
            if is_negative:
                digits = str_val[1:]
            else:
                digits = str_val

            # Ne conserver que les chiffres
            digits = ''.join(ch for ch in digits if ch.isdigit())
            logger.debug(f"[{num_acc}][{coord_name}] Digits after cleaning: '{digits}' (len={len(digits)})")

            # Si vide ou que des zéros, c'est invalide
            if not digits or all(ch == '0' for ch in digits):
                logger.debug(f"[{num_acc}][{coord_name}] -> REJECTED (all zeros)")
                return np.nan

            # Padding à 7 chiffres
            if len(digits) < 7:
                original_digits = digits
                digits = digits.zfill(7)
                logger.debug(f"[{num_acc}][{coord_name}] Padded: '{original_digits}' -> '{digits}'")

            # Format BAAC : DDMMMMM (2 chiffres degrés + 5 chiffres décimales)
            degrees = digits[:2]
            decimals = digits[2:7]

            try:
                coord = float(f"{degrees}.{decimals}")
            except ValueError:
                logger.debug(f"[{num_acc}][{coord_name}] -> REJECTED (ValueError on format)")
                return np.nan

            # Validation stricte
            if coord == 0.0 or coord > 90:
                logger.debug(f"[{num_acc}][{coord_name}] -> REJECTED ancient format (zero or >90): {coord}")
                return np.nan

            final_coord = -coord if is_negative else coord
            logger.debug(f"[{num_acc}][{coord_name}] -> OK ancient: {final_coord}")
            return final_coord

        # Traiter latitude avec logging
        if "lat" in df.columns:
            logger.info("Processing LATITUDE...")
            # Appliquer la fonction avec les métadonnées pour debug
            results = []
            for idx, row in df.iterrows():
                num_acc = row.get('num_acc', f'row_{idx}')
                result = parse_gps_coordinate(row['lat'], 'LAT', num_acc)
                results.append(result)
                # Log seulement les 20 premières lignes pour pas spammer
                if idx >= 20:
                    break
            df["lat"] = df["lat"].apply(lambda x: parse_gps_coordinate(x, 'LAT', ''))
        else:
            df["lat"] = np.nan

        # Traiter longitude
        if "long" in df.columns:
            logger.info("Processing LONGITUDE...")
            df["long"] = df["long"].apply(lambda x: parse_gps_coordinate(x, 'LONG', ''))
        else:
            df["long"] = np.nan

        # LOG : Échantillon des données APRÈS traitement
        if "lat" in df.columns and "long" in df.columns:
            logger.info("DEBUG GPS - Échantillon APRÈS traitement:")
            sample_after = df[["num_acc", "lat", "long"]].head(10)
            for idx, row in sample_after.iterrows():
                logger.info(f"  {row['num_acc']} | lat={row['lat']} | long={row['long']}")

            # Statistiques
            total = len(df)
            with_coords = df[df['lat'].notna() & df['long'].notna()].shape[0]
            logger.info(f"STATS: {with_coords}/{total} accidents avec coordonnées ({100*with_coords/total:.1f}%)")
            logger.info("=" * 80)

        # Validation finale
        if "lat" in df.columns and "long" in df.columns:
            mask_invalid = (
                df["lat"].isna() | df["long"].isna() |
                (df["lat"].abs() > 90) | (df["long"].abs() > 180)
            )
            df.loc[mask_invalid, ["lat", "long"]] = np.nan

        return df

    def load_year(self, year):
        """
        Charge les 4 fichiers pour une année donnée et retourne un dict structuré.
        """
        base_path = os.path.join(self.data_dir, str(year))

        try:
            carac_file = self.find_file(base_path, "caract")
            lieux_file = self.find_file(base_path, "lieux")
            veh_file = self.find_file(base_path, "vehicules")
            usagers_file = self.find_file(base_path, "usagers")

            df_carac = pd.read_csv(carac_file, sep=None, engine="python",
                                  encoding=self.detect_encoding(carac_file), on_bad_lines="skip")
            df_lieux = pd.read_csv(lieux_file, sep=None, engine="python",
                                  encoding=self.detect_encoding(lieux_file), on_bad_lines="skip")
            df_veh = pd.read_csv(veh_file, sep=None, engine="python",
                                encoding=self.detect_encoding(veh_file), on_bad_lines="skip")
            df_usagers = pd.read_csv(usagers_file, sep=None, engine="python",
                                    encoding=self.detect_encoding(usagers_file), on_bad_lines="skip")

            # Normalisation des colonnes
            df_carac = self.normalize_columns(df_carac)
            df_lieux = self.normalize_columns(df_lieux)
            df_veh = self.normalize_columns(df_veh)
            df_usagers = self.normalize_columns(df_usagers)

            # Nettoyage des codes
            for df in [df_carac, df_lieux, df_veh, df_usagers]:
                self.clean_numeric_codes(df)

            # Traitement timestamp et GPS uniquement sur caractéristiques
            df_carac = self.process_timestamp(df_carac, year)
            df_carac = self.process_coordinates(df_carac)

            result = {
                "accidents": df_carac,
                "lieux": df_lieux,
                "vehicules": df_veh,
                "usagers": df_usagers,
                "year": year
            }

            logger.info(f"{year}: {len(df_carac)} accidents, {len(df_lieux)} lieux, "
                       f"{len(df_veh)} véhicules, {len(df_usagers)} usagers")
            return result

        except Exception as e:
            logger.error(f"Erreur lecture {year}: {e}")
            return None

    def find_file(self, path, keyword):
        files = glob.glob(os.path.join(path, f"*{keyword}*.csv"))
        if not files:
            files = glob.glob(os.path.join(path, f"*{keyword.capitalize()}*.csv"))
        if not files:
            raise FileNotFoundError(f"Fichier {keyword} introuvable dans {path}")
        return files[0]

    def load_all_years(self, n_jobs=10, force_reload=False):
        """Charge toutes les années et retourne un dict avec 4 DataFrames séparés."""
        current_signature = self.get_data_signature()
        cache_signature_file = self.cache_file + ".sig"

        if not force_reload and os.path.exists(self.cache_file) and os.path.exists(cache_signature_file):
            with open(cache_signature_file, "r") as f:
                cached_signature = f.read().strip()

            if cached_signature == current_signature:
                logger.info("Cache BAAC complet trouvé, chargement rapide...")
                data = load(self.cache_file)
                logger.info(f"{len(data['accidents'])} accidents, {len(data['lieux'])} lieux chargés")
                return data
            else:
                logger.info("Cache obsolète, rechargement...")
        else:
            logger.info("Pas de cache, chargement complet...")

        year_dirs = glob.glob(os.path.join(self.data_dir, "[12]0[0-9][0-9]"))
        years = sorted([int(os.path.basename(d)) for d in year_dirs])

        if not years:
            raise FileNotFoundError(f"Aucune année trouvée dans {self.data_dir}")

        logger.info(f"Années: {years}")
        logger.info(f"Chargement parallèle (n_jobs={n_jobs})")

        results = Parallel(n_jobs=n_jobs, verbose=10)(
            delayed(self.load_year)(year) for year in years
        )

        all_accidents = []
        all_lieux = []
        all_vehicules = []
        all_usagers = []

        for r in results:
            if r is not None:
                all_accidents.append(r["accidents"])
                all_lieux.append(r["lieux"])
                all_vehicules.append(r["vehicules"])
                all_usagers.append(r["usagers"])

        df_accidents = pd.concat(all_accidents, ignore_index=True)
        df_lieux = pd.concat(all_lieux, ignore_index=True)
        df_vehicules = pd.concat(all_vehicules, ignore_index=True)
        df_usagers = pd.concat(all_usagers, ignore_index=True)

        logger.info(f"TOTAL: {len(df_accidents)} accidents, {len(df_lieux)} lieux, "
                   f"{len(df_vehicules)} véhicules, {len(df_usagers)} usagers")

        data = {
            "accidents": df_accidents,
            "lieux": df_lieux,
            "vehicules": df_vehicules,
            "usagers": df_usagers
        }

        logger.info("Sauvegarde du cache...")
        dump(data, self.cache_file, compress=3)
        with open(cache_signature_file, "w") as f:
            f.write(current_signature)
        logger.info("Cache sauvegardé")

        return data
