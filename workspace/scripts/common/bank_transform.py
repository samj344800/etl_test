#!/usr/bin/env python
# coding: utf-8

# # Transform bank

# In[6]:


import extract_bank
from taux import convertisseur


# In[5]:


def transform_bank(data):
    data["Market Cap(US$ Billion)"]=data["Market Cap(US$ Billion)"].str.replace(r"\[.*\]","")
    data['Market Cap(US$ Billion)'] = data['Market Cap(US$ Billion)'].astype(float)
    data['Market Cap(EURO Billion)'] = data['Market Cap(US$ Billion)'].apply(convertisseur)
    del data['Market Cap(US$ Billion)']
    data['Market_Cap(EUROBillion)'] = round(data['Market Cap(EURO Billion)'], 2)
    return data


# In[ ]:
