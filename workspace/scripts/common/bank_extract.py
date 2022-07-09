#!/usr/bin/env python
# coding: utf-8

# # Extract bank

# In[5]:


import bs4
import html5lib
import requests
import pandas as pd
import urllib
import os
from datetime import datetime


# In[6]:


#Création du dossier raw pour les donnée brutes
path="C://users/maell/ETL_bank"  #A modifier selon l'utilisateur
os.makedirs(path, exist_ok=True)


# In[7]:


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'

def extract_bank(url):
    data=requests.get(url).text
    soup = BeautifulSoup(data, 'html.parser')
    data = pd.DataFrame(columns=['Name','Market Cap(US$ Billion)'])
    
    for row in soup.find_all("tbody")[3].find_all("tr"):
        col = row.find_all("td")
    
        if (col != []):       
            marketcap = col[2].text.strip()
            name = col[1].text.strip()
            datescrap = datetime.today().year
            data = data.append({"Name":name, "Market Cap(US$ Billion)":marketcap, "Date_scrapping" :datescrap}, ignore_index=True)
    
    return data
