# Imports
import bs4
import requests
import os
import gzip
import shutil


# si prog modulaire : faire les imports de bank_extract.py/api_extract.py/immo_extract.py depuis le common
# checker les dossiers
# checker l'API key
# checker les variables


##########
# Dossiers
##########
working_directory=os.getcwd()  #get working dir
print("Dossier de travail actuel : ", working_directory)


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
data=requests.get(url)

# Records datas in file
if data.status_code == requests.codes.ok:
    print("Url atteinte.")
    with open($PWD/data/bank_rang.txt, 'w', encoding='utf-8') as f:  #checker le dossier
        f.write(data.text)
    print("Enregistrement des données terminé.")
else:
    print("Url non accessible.")

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
    with open($PWD/data/api_taux.txt, 'w', encoding='utf-8') as f:  #checker le dossier
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
            with open(path_immo+"/full.csv.gz", "wb") as f:  #checker le dossier
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
        with gzip.open(path_immo+"/full.csv.gz", 'rb') as f_in:  #checker le dossier
            with open(path_immo+"/extracted_full.csv", 'wb') as f_out:  #checker le dossier
                shutil.copyfileobj(f_in, f_out)
        print("Décompression terminée.")
        break
    except:
        print("Erreur lors de la décompression.")
        print("Le fichier est peut être manquent ou corrompus.")
        print("Contactez le noob de la maintenance!")
        break

