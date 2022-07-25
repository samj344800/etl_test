# Imports
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import database_exists, create_database
from config import USER_NAME, PASS_WORD, SGBD_NAME, SGBD_DRIVER, SGBD_PORT, DB_NAME, HOST_NAME

# Script initialisant l'"engine" et la "déclarative base" de sqlalchemy 


# Initialisation sqlalchmy engine
print("Initialisation Sqlalchemy engine :")
engine=create_engine(f"{SGBD_NAME}+{SGBD_DRIVER}://{USER_NAME}:{PASS_WORD}@{HOST_NAME}:{SGBD_PORT}/{DB_NAME}")  # Var a definir dans /config.py
print("Init teminé.")

# Check si DB existante et la crée dans le cas contraire
print(f"Check de l'existance de la DB {DB_NAME} :")
if not database_exists(engine.url):
    create_database(engine.url)
    print(f"DB {DB_NAME} créée avec succès")
else:
   print("DB existante.")

# Initialisation sqlalchemy base
print("Initialisation Sqlalchemy Base :")
Base = declarative_base()
print("Init terminé.")
