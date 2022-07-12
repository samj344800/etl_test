# Imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import insert, select
from base import engine
from table import ppr_values_temp, ppr_values_temp, raw_bank, bank_final, raw_taux, taux_final




#from config import ROOT_PATH
from common.base import engine


##########
# Comments
##########

#fait les imports load_bank/api/immo depuis common

#########
# Dossier
#########

ROOT_PATH="/home/lenobian/Python/test/ETL_bank/workspace"

###########
# Bank Rang
###########

# Load datas from data/raw/bank_rang.csv
df_bank=pd.read_csv(ROOT_PATH+"/data/raw/bank_rang.csv")

# DF to dict
bank_dict=df_bank.to_dict('records')

# Create object to add session bank_raw
for ligne in bank_dict:
    obj=raw_bank(nom=ligne['Name'], date=ligne['Date_scrapping'], market_cap = ligne['MarketCap_EUROBillion']  )

# Adding to session
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    session.add(obj)
    session.commit()

# Save in bank_final with changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    clean_bank=session.query(bank_final.check)  #check existing in bank_final
    changes_to_insert = session.query(raw_bank.nom,raw_bank.market_cap,raw_bank.date).filter(~raw_bank.check.in_(clean_bank))  #check differences raw/final
    stm = insert(bank_final).from_select (['nom','market_cap', 'date'], changes_to_insert)  #define statement to execute
    session.execute(stm)  #execute stm (adding to bank_final)
    session.commit()  # Save


##########
# Api taux
##########

# Load data from data/raw/api_taux.csv
df_api=pd.read_csv(ROOT_PATH+"/data/raw/api_taux.csv")

# DF to dict
api_dict=df_api.to_dict('records')

# Create object to add session raw_taux
for ligne in api_dict:
    obj=raw_taux(nom=ligne['Nom'],rates=ligne['Rates'] )

# Adding to session
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    session.add(obj)
    session.commit()

# Save in taux_raw with changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    clean_taux=session.query(taux_final.c_pr)  #check existing in bank_final
    changes_to_insert = session.query(raw_taux.rates, raw_taux.nom).filter(~raw_taux.c_pr.in_(clean_taux))  #check differences raw/final
    stm = insert(taux_final).from_select (['nom','rates'], changes_to_insert)  #define statement to execute
    session.execute(stm)  #execute stm (adding to bank_final)
    session.commit()  # Save

############
# Ppr Values
############

# Load datas from extrated_full.csv
df_ppr=pd.read_csv(ROOT_PATH+"/data/raw/extracted_full.csv")

# DF to dict
ppr_dict=df_ppr.to_dict('records')

# Create object to add session ppr_value_temp
for row in ppr_dict:
    obj=ppr_values_temp(
        id_mutation=row['id_mutation'],
        date_mutation=row['date_mutation'],
        valeur_fonciere=row['valeur_fonciere'],
        adresse_numero=row['adresse_numero'],
        adresse_suffixe=row['adresse_suffixe'],
        adresse_nom_voie=row['adresse_nom_voie'],
        code_postal=row['code_postal'],
        nom_commune=row['nom_commune'],
        type_local=row['type_local']
    )

# Adding to session
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    session.add(obj)
    session.commit()

# Save in ppr_values_clean with changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    session.add(obj)
    session.commit()

# Delete changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    ppr_id=session.query(ppr_values_temp.id_property)
    session.query(ppr_values_clean).filter(~ppr_values_clean.id_property.in_(ppr_id)).delete(synchronize_session='fetch')  #check diff and delete
    session.commit()
