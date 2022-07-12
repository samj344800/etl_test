# Imports
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import json

#from config import ROOT_PATH

##########
# Comments
##########

# fait les imports de tranform_bank_api_immo depuis common
# voir pour sauvegarder les data dans data/raw pour le load
ROOT_PATH="/home/lenobian/Python/test/ETL_bank/workspace"

################
# Bank transform
################

# Load data from bank_rang.csv
data=pd.read_csv(ROOT_PATH+'/data/source/bank_rang.csv')

# Delete indice in columns
data["Market Cap(US$ Billion)"]=data["Market Cap(US$ Billion)"].str.replace(r"\[.*\]","")

# Convert Dtype
data['Market Cap(US$ Billion)'] = data['Market Cap(US$ Billion)'].astype(float)

# Apply Euros conversion
data['Market Cap(EURO Billion)'] = data['Market Cap(US$ Billion)']  ##.apply(convertisseur)

# Delete UsDollar column
del data['Market Cap(US$ Billion)']

# Round Euros column and delete older
data['MarketCap_EUROBillion'] = round(data['Market Cap(EURO Billion)'], 2)
del data['Market Cap(EURO Billion)']

# Save transformation in raw/bank_rang.csv
data.to_csv(ROOT_PATH+'/data/raw/bank_rang.csv', index=False)  #check code et dossier

###############
# API transform
###############

# Functions
def transform_rates(extract):
    #On transforme les datas en Json
    json = extract
    #'Rates' est un dico et on récupère donc les clés et les valeurs
    dico_cles = json['rates'].keys()
    liste_cles = list(dico_cles)
    dico_valeurs = json['rates'].values()
    liste_valeurs = list(dico_valeurs)
    #Création du DF avec les clés et valeurs de 'Rates'
    df = pd.DataFrame(liste_valeurs,liste_cles)
    #On nomme la colones des valeurs
    df.columns = ['Rates']
    return df

def convertisseur(dollars):  # voir pour réorganiser l'odre (function et valeurs utilisées par Bank) 
    taux_dollars=df.loc[df['Nom'] == "USD", 'Rates'].iloc[0]
    taux = 1/taux_dollars
    valeur_euros = taux*dollars
    return valeur_euros

# Load datas from api_taux.txt
with open(ROOT_PATH+'/data/source/api_taux.txt') as f:  # check dossier/file
    api_taux=json.load(f)

# Create pandas DF
df = transform_rates(api_taux)

# Add column from index
df["Nom"]=df.index

# Save transformation in raw/api_taux.csv
df.to_csv(ROOT_PATH+'/data/raw/api_taux.csv')  # check code et dossier

######################
# Ppr Values Transform
######################

# Création du df
df=pd.read_csv(ROOT_PATH+"/data/source/extracted_full.csv")  # check dossier/file
df=df[['id_mutation', 'date_mutation', 'valeur_fonciere', 'adresse_numero', 'adresse_suffixe', 'adresse_nom_voie',\
        'code_postal', 'nom_commune', 'type_local']].copy()

# Drop dupliactes
df.drop_duplicates(subset=['id_mutation'],inplace=True)

## Valeur foncières
# Drop des lignes valeur_foncière nulles
df.dropna(subset=['valeur_fonciere'], inplace=True)
# Change dtype
df['valeur_fonciere']=df['valeur_fonciere'].astype(int)

## Adresse
# Format adresse_numero
df['adresse_numero']=df['adresse_numero'][df['adresse_numero'].notnull()].astype(int).astype(str)
# Format adresse_nom_voie
df['code_postal']=df['code_postal'][df['code_postal'].notnull()].astype(int).astype(str)

# Fillna 'none' str type
df=df.fillna('none')

# Format Title adresse_nom_voie
df['adresse_nom_voie']=df['adresse_nom_voie'].str.title()

# Save transformation in raw/extracted_full.csv
df.to_csv(ROOT_PATH+"/data/raw/extracted_full.csv", index=False)  # checkcode et dossier
