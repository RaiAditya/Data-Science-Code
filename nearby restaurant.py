import os
import numpy as np
import re
#from pyxlsb import open_workbook as open_xlsb
import time
import math
import pandas as pd
import requests
import ast
from tqdm import tqdm,trange
import math
import pickle
import time
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import spacy
import pandas as pd
from spacy.lang.en.stop_words import STOP_WORDS
from math import sin, cos, sqrt, atan2, radians
from subprocess import check_output
nlp = spacy.load('en_core_web_sm')
eug = pd.read_excel("eug_stores_lat_long.xlsx")
comp = pd.read_excel("Distance between EUG locations and Competitor Locations.xlsx")

#unique_address = list(set(eug['Address']))
unique_address = {k:None for k in comp['Address'].unique()}
for i in trange(len(unique_address)):
    try:
        add=list(unique_address.keys())[i]
        url="https://maps.googleapis.com/maps/api/geocode/json?address="+add+"&key=AIzaSyCzbsaGTcBAfKEGkg0uWswDUA5P1fC0Ogo"
        res=requests.get(url)
        result=json.loads(res.content)
        unique_address[add] = result
    except :
        time.sleep(15)

pickle.dump(unique_address,open("1000_unsure.pkl","wb"))
df2 = comp
#Reading the pickle file and assiging the lat-long along with sure(0),notsure(1) and not available(-1) flags 
res=[]    
with (open("1000_unsure.pkl", "rb")) as openfile:
    res.append(pickle.load(openfile))
a=res[0]
df2['flag_cord_name']=0
vb=[]
for row in tqdm(df2.iterrows()):
    try:
        ind,i=row
        if a[i['Address']]['status']=='OK' and a[i['Address']]['results'][0]['geometry']['location_type']!='APPROXIMATE':
            df2.loc[df2['Address']==i['Address'],'lat_long_name']=[a[i['Address']]['results'][0]['geometry']['location']]
        elif a[i['Address']]['status']=='OK' and a[i['Address']]['results'][0]['geometry']['location_type']=='APPROXIMATE':
            df2.loc[df2['Address']==i['Address'],'lat_long_name']=[a[i['Address']]['results'][0]['geometry']['location']]
            df2.loc[df2['Address']==i['Address'],'flag_cord_name']=1
        else:
            df2.loc[df2['Address']==i['Address'],'flag_cord_name']=-1
    except:
        vb.append(ind)
        df2.loc[df2['Address']==i['Address'],'flag_cord_name']=-1
        
df2.to_csv("com_stores_lat_long.csv")   

com = pd.read_excel("com_stores_lat_long.xlsx")

results = pd.merge(eug, com,how='outer')

# finding the restaurant less than 5 mile. 
full_data = pd.DataFrame()

R = 6373.0
dict_dist1 = {}
dict_names1 = {}
list_dist1 = []
list_names1 = []
sc_dis1 = []
sc_names1 = []
com = pd.read_excel("com_stores_lat_long.xlsx")
eug = pd.read_excel("eug_stores_lat_long.xlsx")
for i in trange(0, len(eug)):
    try:
        lat1 = radians(eug.iloc[i][2])
        lon1 = radians(eug.iloc[i][3])
        name1 = "eug"
        address1 = eug.iloc[i][0]
        #name = df2.iloc[i][3]
        j = 0
        for j in trange(0,len(com)):
            lat2 = radians(com.iloc[j][3])
            lon2 = radians(com.iloc[j][4])
            name2 = com.iloc[j][0]
            address2 = com.iloc[j][1]
            #score = fuzz.partial_ratio(name,name1)
            #score1 = fuzz.token_sort_ratio(name,name1)

            dlon = lon2 - lon1
            dlat = lat2 - lat1
            
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            
            distance = R * c * 0.621371
            
            data = pd.DataFrame(
                {
                 'EUG_RES': name1,
                 'EUG_address' :address1,
                 'compti': name2,
                 'comp_address':address2,
                 'Distance':distance 
                },index=[i*len(com) + j]
                )
            #sc_dis1.append(distance) 
            full_data = full_data.append(data)
            
    except:
        print(i + 10000)
        
full_data.to_csv("rest_Distance_Metric_AR.csv")
            
            """if (distance < 10 ):
                sc_dis1.append(distance)
                sc_names1.append(name1)
                
                #sc_names.append(name1)
                
        dict_dist1[i] = sc_dis1
        dict_names1[i] = sc_names1
        
        list_dist1.append(dict_dist1)
        list_names1.append(dict_names1)
        
        dict_dist1 = {}
        dict_names1 = {}
        
        sc_dis1 = []
        sc_names1 = []"""
            print(i)
    except:
        print(i + 10000)

eug['distance_from eug'] = list_dist1
eug['distance'] = list_names1
eug.to_csv("rest_nearby__eug_AR.csv")      
    