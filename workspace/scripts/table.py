# Imports
from sqlalchemy.orm import column_property
from sqlalchemy import Column, Integer, Float, String
from base import Base

# Script configurant les différentes class sqlalchemy définissant les tables de la DB

#####################
# Define class tables
#####################

# property values temp table
class ppr_values_temp(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'ppr_values_temp'
    id_mutation = Column(String(55), primary_key=True)
    date_mutation = Column(String(55))
    valeur_fonciere = Column(String(15), primary_key=True)
    adresse_numero = Column(String(10))
    adresse_suffixe = Column(String(15))
    adresse_nom_voie = Column(String(55))
    code_postal = Column(String(10))
    nom_commune = Column(String(55))
    type_local = Column(String(55))
    id_property = column_property(id_mutation + ',' + valeur_fonciere)

# property values final table
class ppr_values_clean(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'ppr_values_clean'
    id_mutation = Column(String(55), primary_key=True)
    date_mutation = Column(String(55))
    valeur_fonciere = Column(String(15), primary_key=True)
    adresse_numero = Column(String(10))
    adresse_suffixe = Column(String(15))
    adresse_nom_voie = Column(String(55))
    code_postal = Column(String(10))
    nom_commune = Column(String(55))
    type_local = Column(String(55))
    id_property = column_property(id_mutation + ',' + valeur_fonciere)

# rank bank temp table
class raw_bank(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'raw_bank'
    id=Column(Integer, primary_key=True, autoincrement = True)
    nom=Column(String(55))
    market_cap=Column(String(55))
    date=Column(String(55))
    check = column_property(nom + market_cap + date)

# rank bank final table
class bank_final(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'bank_final'
    id=Column(Integer, primary_key=True, autoincrement = True)
    nom=Column(String(55))
    market_cap=Column(String(55))
    date=Column(String(55))
    check = column_property(nom + market_cap + date)

# taux temp table
class raw_taux(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'raw_taux'
    id=Column(Integer, primary_key=True, autoincrement = True)
    nom=Column(String(55))
    rates=Column(String(55))
    c_pr = column_property(nom + rates)

# taux final table
class taux_final(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'taux_final'
    id=Column(Integer, primary_key=True, autoincrement = True)
    nom=Column(String(55))
    rates=Column(String(55))
    c_pr = column_property(nom + rates)
