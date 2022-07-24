# Imports
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import pandas as pd
import os
import gzip
import shutil
from config import SOURCE_PATH, RAW_PATH

# Script permettant d'extraire les données. Ici sont définis les urls et APIs cibles.


############################
# Variables
############################

#Récupération de l'année au format int
year=int(datetime.today().strftime('%Y'))

######################
# Gestion des dossiers
######################

#Création des dossiers source & raw horodaté
os.makedirs(SOURCE_PATH, exist_ok=True)
os.makedirs(RAW_PATH, exist_ok=True)

###########
# Bank rank
###########

print('Début de l\'extraction des données "Bank Rang".')

# Declare source url
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'

# Request datas from url
data=requests.get(url).text

# Replace the dots below
soup = BeautifulSoup(data, 'html.parser')

# Create df_bank
df_bank = pd.DataFrame(columns=['Name','Market Cap(US$ Billion)', 'Date_scrapping'])

# Fill df
for row in soup.find_all("tbody")[3].find_all("tr"):
    col = row.find_all("td")

    if (col != []):
        marketcap = col[2].text.strip()
        name = col[1].text.strip()
        datescrap = datetime.today().year
        df_bank = df_bank.append({"Name":name, "Market Cap(US$ Billion)":marketcap, "Date_scrapping" :datescrap}, ignore_index=True)

# Records datas in csv
df_bank.to_csv(SOURCE_PATH+'/bank_rang.csv', index=False)

print('Extracttion "Bank Rang" terminée')

##########
# API taux
##########

print('Début de l\'extraction des données "API Taux".')

# Declare source url
url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=nJVuiVk2iIbNtdqdb1ohnpsseVwNXASo"

# Request data from API
r = requests.get(url)

# Records datas in file
if r.status_code == requests.codes.ok:
    print("API atteinte.")
    with open(SOURCE_PATH+'/api_taux.txt', 'w', encoding='utf-8') as f:
        f.write(r.text)
    print("Enregistrement des données terminé.")
else:
    print("API non accessible.")

############
# Ppr Values
############

print('Début de l\'extraction des données "Ppr Values".')

# Declare source url
url="https://files.data.gouv.fr/geo-dvf/latest/csv/"

# Download data file
while True:
    url_test = requests.get(url+str(year)+"/full.csv.gz", stream=True)
    if url_test.status_code==200:  #test reponse du serveur
        print("le fichier le plus récent est :",url+str(year)+"/full.csv.gz")
        print("Téléchargement en cours.")
        with requests.get(url+str(year)+"/full.csv.gz", stream=True) as r:
            with open(SOURCE_PATH+"/full.csv.gz", "wb") as f:
                shutil.copyfileobj(r.raw, f) #Enregistrement du fichier (méthode + rapide)
        print("Téléchargement terminé.")
        break
    else:
        year-=1
    if year==1990:
        print("Erreur de connection.")
        print("Les données immobilières n'ont pas été mises à jour!")
        print("Contactez le noob de la maintenance!")
        break

# Extract Datas
#Décompression du fichier et gestion d'erreur
while True:
    try:
        print("Décompression du fichier.")
        with gzip.open(SOURCE_PATH+"/full.csv.gz", 'rb') as f_in:
            with open(SOURCE_PATH+"/extracted_full.csv", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Décompression terminée.")
        break
    except:
        print("Erreur lors de la décompression.")
        print("Le fichier est peut être manquent ou corrompus.")
        print("Contactez le noob de la maintenance!")
        break
