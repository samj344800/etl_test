# Imports
import pandas as pd


from xxx.py import session

##########
# Comments
##########

#fait les imports load_bank/api/immo depuis common

#########
# Dossier
#########

#from execute.py import wd_path

###########
# Bank Rang
###########

# Load datas from data/raw/bank_rang.csv
df_bank=pd.read_csv(wd_path+"/data/raw/bank_rang.csv")

# DF to dict
bank_dict=df_bank.to_dict('records')

# Create object to add session
for ligne in test:
    obj=raw_bank(nom=ligne['Name'], date=ligne['Date_scrapping'], market_cap = ligne['MarketCap_EUROBillion']  )

# Adding to session
with session as session:  #initialize session in with statement to prevent connection errors
    session.add(obj)
    session.commit()


