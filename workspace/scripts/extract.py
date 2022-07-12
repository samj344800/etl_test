# Imports
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import pandas as pd
import os
import gzip
import shutil
#from config import ROOT_PATH


# si prog modulaire : faire les imports de bank_extract.py/api_extract.py/immo_extract.py depuis le common
# checker les dossiers
# checker l'API key
# checker les variables


##########
# Var test
##########
ROOT_PATH="/home/lenobian/Python/test/ETL_bank/workspace"


###########
# Variables
###########
year=2021  #temp var

###########
# Bank rank
###########

print('Début de l\'extraction des données "Bank Rank".')

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
df_bank.to_csv(ROOT_PATH+'/data/source/bank_rang.csv', index=False)

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
    with open(ROOT_PATH+'/data/source/api_taux.txt', 'w', encoding='utf-8') as f:  #checker le dossier
        f.write(r.text)
    print("Enregistrement des données terminé.")
else:
    print("API non accessible.")

############
# Ppr Values
############

# Declare source url
url="https://files.data.gouv.fr/geo-dvf/latest/csv/"

# Download data file
while True:
    url_test = requests.get(url+str(year)+"/full.csv.gz", stream=True)  #checker les variables
    if url_test.status_code==200:  #test reponse du serveur
        print("le fichier le plus récent est :",url+str(year)+"/full.csv.gz")
        print("Téléchargement en cours.")
        with requests.get(url+str(year)+"/full.csv.gz", stream=True) as r:
            with open(ROOT_PATH+"/data/source/full.csv.gz", "wb") as f:  #checker le dossier
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
        with gzip.open(ROOT_PATH+"/data/source/full.csv.gz", 'rb') as f_in:  #checker le dossier
            with open(ROOT_PATH+"/data/source/extracted_full.csv", 'wb') as f_out:  #checker le dossier
                shutil.copyfileobj(f_in, f_out)
        print("Décompression terminée.")
        break
    except:
        print("Erreur lors de la décompression.")
        print("Le fichier est peut être manquent ou corrompus.")
        print("Contactez le noob de la maintenance!")
        break

