# Imports
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

# Script initialisant l'"engine" et la "déclarative base" de sqlalchemy 


### Peut-être créer ici des input pour ou une ref a un fichier config
### pour le chemin d'accès à la db 


# Initialize sqlalchmy engine
engine=create_engine("mysql+pymysql://user:mdp:3306/db_name")

# Initialize sqlalchemy base
Base = declarative_base()
