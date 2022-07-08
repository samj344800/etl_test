#!/usr/bin/env python
# coding: utf-8

# # Load bank

# In[3]:


from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Float, String
import pymysql.cursors
from sqlalchemy.orm import column_property
from sqlalchemy.dialects.postgresql import insert


# In[4]:


Base = declarative_base()
engine=create_engine("mysql+pymysql://root:Aline150421!@localhost:3306/bank")
session = Session(engine)

class raw_bank(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'raw_bank'
    id=Column(Integer, primary_key=True, autoincrement = True)
    nom=Column(String(55))
    market_cap=Column(String(55))
    date=Column(String(55))
    check = column_property(nom + market_cap + date)
    
class bank_final(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'bank_final'
    id=Column(Integer, primary_key=True, autoincrement = True)
    nom=Column(String(55))
    market_cap=Column(String(55))
    date=Column(String(55))
    check = column_property(nom + market_cap + date)

    
def load(Base):
    for table in Base.metadata.tables:
        Base.metadata.create_all(engine)
        
    bank_convert=data.to_dict('records')
    
    session=Session(engine)
    
    for ligne in test:
        obj=raw_bank(nom=ligne['Name'], date=ligne['Date_scrapping'], market_cap = ligne['MarketCap_EUROBillion']  )
        session.add(obj)

    session.commit()

    
    #insérer des éléments de la table finale
    clean_bank=session.query(bank_final.check)
    changes_to_insert = session.query(raw_bank.nom,raw_bank.market_cap,raw_bank.date).filter(~raw_bank.check.in_(clean_bank)) 
    stm = insert(bank_final).from_select (['nom','market_cap', 'date'], changes_to_insert)
    session.execute(stm)
    session.commit()
    
    return Base
