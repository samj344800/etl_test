import os
import datetime

# Script permettant de définir l'environnement du programme
# Utile notemment pour définir /scripts/base.py: <create_engine(f"{SGBD_NAME}+{SGBD_DRIVER}://{USER_NAME}:{PASS_WORD}@{HOST_NAME}:{SGBD_PORT}/{DB_NAME}")>

# User (SQL server)
USER_NAME='root'
PASS_WORD='Root01Oracle!'

# SQL Server
SGBD_NAME='mysql'
SGBD_DRIVER='pymysql'
HOST_NAME='localhost'
SGBD_PORT=3306
DB_NAME='etl_db'

# Récuperation de la date du jour
date=datetime.datetime.today().strftime('%Y-%m-%d')

# Environnement
ROOT_PATH=os.getcwd()  # Dossier racine d'éxécition du programme
SOURCE_PATH=ROOT_PATH+"/data/source/Downloaded_at_"+date  # Dossier données brutes
RAW_PATH=ROOT_PATH+"/data/raw/downloaded_at_"+date  # Dossier données tranformées
