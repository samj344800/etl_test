# Imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import insert, select
from scripts.base import engine
from scripts.table import ppr_values_temp, ppr_values_clean, raw_bank, bank_final, raw_taux, taux_final
from config import RAW_PATH

###########
# Bank Rang
###########

print('Loading "Bank rang" datas in DB')

# Load datas from data/raw/bank_rang.csv
df_bank=pd.read_csv(RAW_PATH+"/bank_rang.csv")

# DF to dict
bank_dict=df_bank.to_dict('records')

# Create object, add and commit to bank_raw
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    for ligne in bank_dict:
        obj=raw_bank(nom=ligne['Name'], date=ligne['Date_scrapping'], market_cap = ligne['MarketCap_EUROBillion']  )
        session.add(obj)
    session.commit()

# Save in bank_final with changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    clean_bank=session.query(bank_final.check)  #check existing in bank_final
    changes_to_insert = session.query(raw_bank.nom,raw_bank.market_cap,raw_bank.date).filter(~raw_bank.check.in_(clean_bank))  #check differences raw/final
    stm = insert(bank_final).from_select (['nom','market_cap', 'date'], changes_to_insert)  #define statement to execute
    session.execute(stm)  #execute stm (adding to bank_final)
    session.commit()  # Save

print('"Bank_rang" loaded')

##########
# Api taux
##########

print('Loading "API Taux" datas in DB')

# Load data from data/raw/api_taux.csv
df_api=pd.read_csv(RAW_PATH+"/api_taux.csv")

# DF to dict
api_dict=df_api.to_dict('records')

# Create object, add and commit to raw_taux
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    for ligne in api_dict:
        obj=raw_taux(nom=ligne['Nom'],rates=ligne['Rates'] )
        session.add(obj)
    session.commit()

# Save in taux_raw with changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    clean_taux=session.query(taux_final.c_pr)  #check existing in bank_final
    changes_to_insert = session.query(raw_taux.rates, raw_taux.nom).filter(~raw_taux.c_pr.in_(clean_taux))  #check differences raw/final
    stm = insert(taux_final).from_select (['nom','rates'], changes_to_insert)  #define statement to execute
    session.execute(stm)  #execute stm (adding to bank_final)
    session.commit()  # Save

print('"API Taux" loaded')

############
# Ppr Values
############

print('Loading "Ppr Values" datas in DB')

# Load datas from extrated_full.csv
df_ppr=pd.read_csv(RAW_PATH+"/extracted_full.csv")

# DF to dict
ppr_dict=df_ppr.to_dict('records')

# Create object, add and commit to ppr_values_temp
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
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
        session.add(obj)
    session.commit()

# Save in ppr_values_clean with changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    cleaned_ppr_values=session.query(ppr_values_clean.id_property)
    changes_to_insert = session.query(ppr_values_temp.id_mutation,ppr_values_temp.date_mutation,ppr_values_temp.valeur_fonciere,ppr_values_temp.adresse_numero,ppr_values_temp.adresse_suffixe,ppr_values_temp.adresse_nom_voie,ppr_values_temp.code_postal,ppr_values_temp.nom_commune,ppr_values_temp.type_local).filter(~ppr_values_temp.id_property.in_(cleaned_ppr_values))     
    stm = insert(ppr_values_clean).from_select(['id_mutation','date_mutation','valeur_fonciere','adresse_numero','adresse_suffixe','adresse_nom_voie','code_postal','nom_commune','type_local'],changes_to_insert)
    session.execute(stm)
    session.commit()

# Delete changes if needed
with Session(engine) as session:  #initialize session in with statement to prevent connection errors
    ppr_id=session.query(ppr_values_temp.id_property)
    session.query(ppr_values_clean).filter(~ppr_values_clean.id_property.in_(ppr_id)).delete(synchronize_session='fetch')  #check diff and delete
    session.commit()

print('"Ppr Values" loaded')
print("**********")

# Clean temp tables
print("Nettoyage des tables temporaires :")
with Session(engine) as session:
    ppr_values_temp.__table__.drop(engine)
    print("ppr_value_temp dropped!")
    raw_bank.__table__.drop(engine)
    print("raw_bank dropped!")
    raw_taux.__table__.drop(engine)
    print("raw_taux dropped!")
