# Imports
from scripts.base import Base, engine
from scripts.table import *
from config import DB_NAME

# Script permettant de créer les tables de la DB

##############
# Create table
##############

if __name__ == "__main__":
    print(f"Creation {DB_NAME}_tables :")
    Base.metadata.create_all(engine)
    print("Creation terminée.")
