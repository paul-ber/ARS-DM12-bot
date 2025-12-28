import pandas as pd
import os
import glob

class BAACLoader:
    def __init__(self, data_dir="data/raw"):
        self.data_dir = data_dir

    def load_year(self, year):
        """Charge et fusionne les 4 fichiers pour une année donnée"""
        base_path = os.path.join(self.data_dir, str(year))

        print(f"🔄 Chargement année {year} depuis {base_path}...")

        # 1. Lecture des 4 fichiers (Sép ; pour années récentes > 2019)
        # Gestion des potentiels noms de fichiers (parfois majuscules/minuscules)
        try:
            df_carac = pd.read_csv(self._find_file(base_path, "caract"), sep=';', encoding='utf-8', low_memory=False)
            df_lieux = pd.read_csv(self._find_file(base_path, "lieux"), sep=';', encoding='utf-8', low_memory=False)
            df_veh = pd.read_csv(self._find_file(base_path, "vehicules"), sep=';', encoding='utf-8', low_memory=False)
            df_usagers = pd.read_csv(self._find_file(base_path, "usagers"), sep=';', encoding='utf-8', low_memory=False)
        except FileNotFoundError as e:
            print(f"❌ Erreur: {e}")
            return None

        # 2. Nettoyage clefs de jointure
        for df in [df_carac, df_lieux, df_veh, df_usagers]:
            df['Num_Acc'] = df['Num_Acc'].astype(str)

        # 3. Aggregation des USAGERS (1 ligne par accident)
        # On veut savoir : Gravité max, Age moyen, Nb tués
        print("   ↳ Aggregation Usagers...")

        def agg_usagers(x):
            nb_tues = (x['grav'] == 2).sum() # 2 = Tué
            nb_blesses_graves = (x['grav'] == 3).sum() # 3 = Hospitalisé
            gravite_max = x['grav'].min() # 1=Indemne... 4=Léger. Attention ordre BAAC bizarre.
            # Correction ordre gravité pour être logique (4=Mortel, 3=Grave, 2=Léger, 1=Indemne)
            # BAAC actuel : 2=Tué, 3=Hosp, 4=Léger, 1=Indemne

            # On va créer notre propre score de gravité
            is_mortel = 2 in x['grav'].values
            is_grave = 3 in x['grav'].values

            score_gravite = "Mortel" if is_mortel else ("Grave" if is_grave else "Léger")

            return pd.Series({
                'nb_tues': nb_tues,
                'nb_graves': nb_blesses_graves,
                'gravite_accident': score_gravite,
                'age_moyen_usagers': (year - x['an_nais']).mean()
            })

        usagers_agg = df_usagers.groupby('Num_Acc').apply(agg_usagers).reset_index()

        # 4. Aggregation des VÉHICULES (1 ligne par accident)
        # On veut savoir : Types impliqués (ex: est-ce qu'il y a une moto ?)
        print("   ↳ Aggregation Véhicules...")

        def agg_vehicules(x):
            # Codes catv (Catégorie véhicule)
            cats = x['catv'].unique()
            # 30,31,32,33,34 = Moto/Scooter
            implique_moto = any(c in [30,31,32,33,34, 1, 2] for c in cats) # 1,2 anciens codes
            # 07 = VL
            implique_pl = any(c in [13,14,15,16,17] for c in cats)
            # 01 = Vélo
            implique_velo = 1 in cats

            return pd.Series({
                'implique_moto': implique_moto,
                'implique_pl': implique_pl,
                'implique_velo': implique_velo,
                'nb_vehicules': len(x)
            })

        veh_agg = df_veh.groupby('Num_Acc').apply(agg_vehicules).reset_index()

        # 5. Fusions finales
        print("   ↳ Merging final...")
        # Carac + Lieux (1-1)
        df_final = pd.merge(df_carac, df_lieux, on='Num_Acc', how='left')
        # + Usagers aggrégés
        df_final = pd.merge(df_final, usagers_agg, on='Num_Acc', how='left')
        # + Véhicules aggrégés
        df_final = pd.merge(df_final, veh_agg, on='Num_Acc', how='left')

        # 6. Création Timestamp propre
        # hrmn est parfois "850" pour 08:50 ou "1430"
        df_final['hrmn'] = df_final['hrmn'].astype(str).str.replace(':', '').str.zfill(4)
        # Gestion des erreurs de saisie (ex: 2400 ou 9999)
        df_final = df_final[df_final['hrmn'].str.match(r'^([0-1]?[0-9]|2[0-3])[0-5][0-9]$')]

        df_final['heure'] = df_final['hrmn'].str[:2].astype(int)
        df_final['minute'] = df_final['hrmn'].str[2:].astype(int)

        # Colonne 'an' est parfois 22, parfois 2022. On normalise.
        df_final['an'] = df_final['an'].apply(lambda x: x + 2000 if x < 100 else x)

        df_final['timestamp'] = pd.to_datetime(
            df_final['an'].astype(str) + '-' +
            df_final['mois'].astype(str).str.zfill(2) + '-' +
            df_final['jour'].astype(str).str.zfill(2) + ' ' +
            df_final['heure'].astype(str).str.zfill(2) + ':' +
            df_final['minute'].astype(str).str.zfill(2),
            errors='coerce'
        )

        # Nettoyage coordonnées (virgule -> point)
        # Parfois lat/long sont vides ou 0
        df_final['lat'] = df_final['lat'].astype(str).str.replace(',', '.')
        df_final['long'] = df_final['long'].astype(str).str.replace(',', '.')
        df_final['lat'] = pd.to_numeric(df_final['lat'], errors='coerce')
        df_final['long'] = pd.to_numeric(df_final['long'], errors='coerce')

        # Filtre les coordonnées invalides (France métropole approx)
        df_final = df_final[
            (df_final['lat'] > 41) & (df_final['lat'] < 52) &
            (df_final['long'] > -6) & (df_final['long'] < 10)
        ]

        print(f"✅ Année {year} chargée : {len(df_final)} accidents valides.")
        return df_final

    def _find_file(self, path, keyword):
        """Cherche un fichier contenant le keyword dans le dossier"""
        # Ex: cherche *caracteristiques*.csv
        files = glob.glob(os.path.join(path, f"*{keyword}*.csv"))
        if not files:
            # Essai avec majuscule
            files = glob.glob(os.path.join(path, f"*{keyword.capitalize()}*.csv"))

        if not files:
            raise FileNotFoundError(f"Fichier '{keyword}' introuvable dans {path}")
        return files[0]
