# Imports
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
#from config import USER_NAME PASS_WORD SGBD_NAME SGBD_DRIVER SGBD_PORT

# Script initialisant l'"engine" et la "déclarative base" de sqlalchemy 


### Peut-être créer ici des input pour ou une ref a un fichier config
### pour le chemin d'accès à la db 


# Initialize sqlalchmy engine
engine=create_engine("mysql+pymysql://root:Root01Oracle!@localhost:3306/etl_db")  #définir variable user/pass/port dans config.py

# Initialize sqlalchemy base
Base = declarative_base()
