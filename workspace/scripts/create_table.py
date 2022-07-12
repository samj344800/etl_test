# Imports
from base import Base, engine
from table import *

# Script permettant de créer les tables de la DB

##############
# Create table
##############

if __name__ == "__main__":
    Base.metadata.create_all(engine)
