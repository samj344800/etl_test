# Imports
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from config import USER_NAME, PASS_WORD, SGBD_NAME, SGBD_DRIVER, SGBD_PORT, DB_NAME, HOST_NAME

# Script initialisant l'"engine" et la "déclarative base" de sqlalchemy 


# Initialisation sqlalchmy engine
print("Initialisation Sqlalchemy engine :")
engine=create_engine(f"{SGBD_NAME}+{SGBD_DRIVER}://{USER_NAME}:{PASS_WORD}@{HOST_NAME}:{SGBD_PORT}/{DB_NAME}")  # Var a definir dans /config.py
print("Init teminé.")

# Initialisation sqlalchemy base
print("Initialisation Sqlalchemy Base :")
Base = declarative_base()
print("Init terminé.")
