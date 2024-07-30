import pandas as pd
import pytz
from haversine import haversine, Unit
import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

df_aircraft = pd.read_csv("data/ADSB_Aircrafts_filtered.csv", delimiter=";")
df_companies = pd.read_csv("data/ADSB.Companies_filtered.csv", delimiter=";", encoding='latin1')
df_cat = pd.read_csv("data/ADSB_CAT.csv", delimiter=";", encoding='ascii')


# mise en majuscule et supprime les espaces avant et après
df_aircraft['Fabricant'] = df_aircraft['Fabricant'].str.upper().str.strip()

# Nom des modèles en majuscule
df_aircraft['Modele'] = df_aircraft['Modele'].str.upper()

set_airport_lat = 50.5619
set_airport_lon = 3.0894
set_airport = (set_airport_lat, set_airport_lon)

# Étape 1: Connexion à MongoDB et récupération des documents
client = MongoClient(os.getenv("MONGO_URL"))
db = client[os.getenv("MONGO_DB")]
collection = db[os.getenv("MONGO_RAW_COLLECTION")]

documents = list(collection.find())

# Étape 2: Transformation des documents
data = []
for doc in documents:
    flattened_doc = {}
    flattened_doc["ts"] = doc["ts"]
    for key, value in doc.get('meta', {}).items():
        flattened_doc[f"meta.{key}"] = value if value != "" else None
    for key, value in doc.get('value', {}).items():
        flattened_doc[f"value.{key}"] = value if value != "" else None
    data.append(flattened_doc)

# Étape 3: Création du DataFrame pandas
df = pd.DataFrame(data)


# Remplir les valeurs manquantes dans la colonne 'flight' avec un bfill
df['meta.flight'] = df.sort_values('ts').groupby('meta.hex')['meta.flight'].transform(lambda x: x.bfill())
# Remplir les valeurs manquantes dans la colonne 'flight' avec un Ffill
df['meta.flight'] = df.sort_values('ts').groupby('meta.hex')['meta.flight'].transform(lambda x: x.ffill())

# Remplir les valeurs manquantes dans la colonne 'CATEGORY' en utilisant les valeurs non manquantes basées sur 'Hex_number'
df['meta.category'] = df.sort_values('ts').groupby('meta.hex')['meta.category'].transform(lambda x: x.bfill())
# Remplir les valeurs manquantes dans la colonne 'category' dans les cas il faut un Ffill
df['meta.category'] = df.sort_values('ts').groupby('meta.hex')['meta.category'].transform(lambda x: x.ffill())


# Merge pour avoir les Fabricants et Modeles des aeronefs
df = df.merge(df_aircraft, how='left', on='meta.hex')

# Suppression des aeroplaner, ULM et des ballons

# CIBLAGE DES BALL
BALL = df['Modele'] == 'BALL'

# Suppression des lignes avec Ball
df = df.drop(df[BALL].index)


# SUPPRESSION ULAC

# CIBLAGE DES ULAC
ULAC = df['Modele'] == 'ULAC'

# Suppression des lignes avec ULAC
df = df.drop(df[ULAC].index)


# CIBLAGE DES GLID
GLID = df['Modele'] == 'GLID'

# Suppression des lignes avec GLID
df = df.drop(df[GLID].index)

# DETECTION DES AVIONS QUI ONT DES ALTITUDES BASSES
avion_basse_altitude = df.sort_values('ts').groupby(['Fabricant', 'Modele'])['value.alt_geom'].max().reset_index()
avion_basse_altitude = avion_basse_altitude[avion_basse_altitude['value.alt_geom'] < 5000]

# Identifier les petits avions dans ADSB_join
filtre_basse_altitute = df['Modele'].isin(avion_basse_altitude['Modele'])

# Supprimer les lignes des petits avions
df = df[~filtre_basse_altitute]

# Conversion des altitudes en mètre
df["alt_geom"] = (df["value.alt_geom"] / 3.281).astype(int)


# avoir les catégories des avions avant la mi-mai
# Création de la colonne Fabricant_modele pour merge sur la clé commune
df.Fabricant = df.Fabricant.apply(str)
df.Modele = df.Modele.apply(str)
df['Fabricant_Modele'] = df[['Fabricant', 'Modele']].agg(' '.join, axis=1)

# jointure des tables
df = df.merge(df_cat, how='left', on='Fabricant_Modele')

# mise en forme des colonnes
df["meta.category"] = df["meta.category"].fillna(df["category"])
df = df.drop(columns=['Fabricant_Modele', 'Fabricant_y', "category", "Modele_y"])
df = df.rename(columns={'Fabricant_x': 'meta.Fabricant', 'Modele_x': 'meta.Modele'})

df['meta.category'].value_counts()

# Suppression des catégories qui ne seront pas analysées et ne garder que les categories A1,A2,A3,A5
df = df[df['meta.category'].isin(["A1", "A2", "A3", "A4", "A5"])]

# Ajouter les nom_compagnies
df['OACI'] = df['meta.flight'].str[:3]
df['OACI'] = df['OACI'].astype(str)
df_companies['OACI'] = df_companies['OACI'].astype(str)

# df_companies = df_companies[['OACI', 'Indicatif','Nom']]
# df_companies = df_companies[df_companies['OACI'].notna()]
df = df.merge(df_companies, how='left', on='OACI')

# Correction des écritures pour Air Algérie et Pantanal Linhas Aéreas
df['Nom_compagnie'] = df['Nom_compagnie'].str.replace(r'(?i)air alg.*', 'Air Algerie', regex=True)
df['Nom_compagnie'] = df['Nom_compagnie'].str.replace(r'(?i)pantanal linhas.*', 'Pantanal Linhas Aereas', regex=True)

# Définir le fuseau horaire de la France
france = pytz.timezone('Europe/Paris')

# Convertir les timestamps Unix en objets Timestamp avec le fuseau horaire UTC
df['ts'] = pd.to_datetime(df['ts'], utc=True)

# Convertir les objets Timestamp en heure française
df['ts'] = df['ts'].dt.tz_convert(france)


# Calcul de la distance de chaque points jusqu'a l'aéroport de Lesquin
def calculate_distance(row):
    plane_lat = float(row['value.lat-avion'])
    plane_lon = float(row['value.lon-avion'])
    distance = haversine((plane_lat, plane_lon), set_airport, unit=Unit.KILOMETERS)
    return distance


df['value.distance'] = df.apply(calculate_distance, axis=1)

# Définition des créneaux horaires (Jour, soir, nuit)
df['meta.Heures'] = df['ts'].dt.hour


def determine_creneaux(hour):
    if 6 <= hour < 18:
        return 'jour'
    elif 18 <= hour < 22:
        return 'soir'
    else:
        return 'nuit'


df['meta.creneaux'] = df['meta.Heures'].apply(determine_creneaux)
df = df.drop(columns=['meta.Heures'])

# Création du tag pour différencier les avions qui peuvent sembler être le meme mais qui ne le sont pas
df['ts_date'] = df['ts'].dt.date
df['delta'] = df.sort_values(by=['ts']).groupby(['meta.hex', 'meta.flight', 'ts_date'])['ts'].diff()
df['tag'] = df.delta > timedelta(minutes=2)
df['tag'] = df.sort_values(by=['ts']).groupby(['meta.hex', 'meta.flight', 'ts_date'])['tag'].cumsum()


# Filtrer avec un masque que les avions qui ont une distance minimale de l'aeroport de moins de 10KM
group_dist = df.groupby(['meta.hex', 'meta.flight', 'ts_date', 'tag'])['value.distance'].min().reset_index()
filtre_min_dist = group_dist[group_dist['value.distance'] < 10]

mask = (
    (df['meta.hex'].isin(filtre_min_dist['meta.hex'])) &
    (df['meta.flight'].isin(filtre_min_dist['meta.flight'])) &
    (df['ts_date'].isin(filtre_min_dist['ts_date'])) &
    (df['tag'].isin(filtre_min_dist['tag']))
)

df = df[mask]

df.count()

# Créez un DataFrame dictionnaire
df = pd.DataFrame({
    'ts': df['ts'],
    'meta.hex': df['meta.hex'],
    'meta.flight': df['meta.flight'],
    'meta.Nom_compagnie': df['Nom_compagnie'],
    'meta.category': df['meta.category'],
    'meta.Fabricant': df['meta.Fabricant'],
    'meta.Modele': df['meta.Modele'],
    'meta.creneaux': df['meta.creneaux'],
    'meta.tag': df['tag'],
    'value.alt_geom': df['alt_geom'],
    'value.lon-avion': df['value.lon-avion'],
    'value.lat-avion': df['value.lat-avion'],
    'value.mach': df['value.mach'],
    'value.distance': df['value.distance']
})


# Convertir le DataFrame en une liste de dictionnaires
# data_dict = df.to_dict("records")
df.reset_index(inplace=True)

data_dict = []

for k in range(df.shape[0]):
    data_dict.append({
        "ts": df.loc[k, 'ts'],
        "meta": {
            "hex": df.loc[k, 'meta.hex'],
            "flight": df.loc[k, 'meta.flight'],
            'Nom_compagnie': df.loc[k, 'meta.Nom_compagnie'],
            'category': df.loc[k, 'meta.category'],
            'Fabricant': df.loc[k, 'meta.Fabricant'],
            'Modele': df.loc[k, 'meta.Modele'],
            'creneaux': df.loc[k, 'meta.creneaux'],
            'tag': int(df.loc[k, 'meta.tag'])
        },
        "value": {
            'alt_geom': int(df.loc[k, 'value.alt_geom']),
            'lon-avion': df.loc[k, 'value.lon-avion'],
            'lat-avion': df.loc[k, 'value.lat-avion'],
            'mach': df.loc[k, 'value.mach'],
            'distance': df.loc[k, 'value.distance']
        }
    })


prod = db[os.getenv("MONGO_CLEAN_COLLECTION")]  # Utile à la fin du script


# Insérer plusieurs documents
try:
    prod.insert_many(data_dict)
    print("Données insérées avec succès")
except pymongo.errors.OperationFailure as e:
    print(f"Erreur d'insertion : {e.details['errmsg']}")
