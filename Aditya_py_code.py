# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import time
time1 = time.time()
import warnings
warnings.simplefilter(action='ignore')
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
import datetime
import pandas as pd
import time
import numpy as np
import re
import sys


connection_db = 1
fb_connection = 1
flag = 1
data_present = 1
access_token = 'EAAD4BZAl5iDABAA2U4fpmOvU35tmmxeELuGMmD6vrADVjh30PZClc0IJx7uRj3xuvE4xF4ZCNlzYVM7jRETeant0FFETpPpFZAZCQR1QXDNKvxTonShZBuSQuNXIAYUBe7T0jQrFvWnC6Yt4aloIF272qRYOjIPZAVg8Evp4SbIx6uFqBuF6WzGxZA9LBjRczoMZD'
ad_account_id = 'act_490659701459349'
app_secret = '2a18335ee2f12d6209179f7960058306'
app_id = '272702933403696'
page_id = '301959993192748'

now_time = int(round(time.time() * 1000)) - 60*60*120
time_one_hour = now_time - 60*60

try:
    FacebookAdsApi.init(access_token=access_token)
    fields = ['leads']
    params = {
            'from_date': str(time_one_hour),'to_date': str(now_time)
    }
    c = AdAccount(page_id)
    
    
    
    a = c.get_lead_gen_forms(fields = fields)
    status = "connection established"
    print(status)
    fb_connection = 1
except:
    status = "access token expires or connection with fb not made"
    print(status)
    fb_connection = 0
    flag = 0
    print("Full code run status:" ,flag, "\nfb connection status:", fb_connection)
    sys.exit()
    
b = list(a)
if len(b) <= 0:
    status = "Did not recieved any data"
    flag = 0
    print("Full code run status:" ,flag, "\nfb connection status:", fb_connection  ,"\ndata_present status",data_present, "\nmysql connection status:" , connection_db)
    sys.exit()
else:
    s=pd.DataFrame(b)
    fff = pd.DataFrame()
    for i in range(len(s)-2):
        user_dict = s.iloc[i]['leads']['data']
        new_dict=[]
        for j in user_dict:
            [p.update({"id":j['id'],"created_time":j['created_time']}) for p in j['field_data']]
            new_dict.append(j['field_data'])
        ff=pd.DataFrame([item for sublist in new_dict for item in sublist])
        fff = fff.append(ff)
    status = "Data recieved"
    print(status)
    


try:
    final_data = fff.sort_values(by=['created_time', 'name'],ascending=[0,0]).groupby(['id','created_time'])['values'].apply(lambda fff: fff.reset_index(drop=True)).unstack().reset_index()
    final_data.columns = ['id', 'Create_time','state','Phone_number','last_name','first_name','email','city']

    #final_data_filtered = final_data[final_data['Create_time'].dtype]
    
    final_data = final_data.replace(np.nan, '', regex=True)
    final_data['Create_time'] = final_data['Create_time'].map(lambda x: x.lstrip('+-][').rstrip('+-]['))
    final_data['email'] = final_data['email'].map(lambda x: str(x).lstrip("'+-]['").rstrip("'+-]['"))
    final_data['first_name'] = final_data['first_name'].map(lambda x: str(x).lstrip("'+-]['").rstrip("'+-]['"))
    final_data['last_name'] = final_data['last_name'].map(lambda x: str(x).lstrip("'+-]['").rstrip("'+-]['"))
    final_data['Phone_number'] = final_data['Phone_number'].map(lambda x: str(x).lstrip("'+-]['").rstrip("'+-]['"))
    final_data['city'] = final_data['city'].map(lambda x: str(x).lstrip("'+-]['").rstrip("'+-]['"))
    final_data['state'] = final_data['state'].map(lambda x: str(x).lstrip("'+-]['").rstrip("'+-]['"))
    
    final_data.loc[final_data["city"] == '','city'] = final_data["state"]
    """state = [' ak', ' al', ' ar', ' az', ' ca', ' co', ' ct', ' dc', ' de', ' fl', ' ga', ' hi', ' ia', ' id', ' il', ' in', ' ks', ' ky', ' la', ' ma', ' md', ' me', ' mh', ' mi', ' mn', ' mo', ' ms', ' mt', ' nc', ' nd', ' ne', ' nh', ' nj', ' nm', ' nv', ' ny', ' oh', ' ok', ' or', ' pa', ' ri', ' sc', ' sd', ' tn', ' tx', ' ut', ' va', ' vt', ' wa', ' wi', ' wv', ' wy']

    temp = list(final_data['city'])
    no_integers = [x for x in temp if not isinstance(x, int)]
    final_data['temp'] = no_integers


    for i in state:
        final_data['temp'] = final_data['temp'].apply(lambda x: x[len(x)-3:len(x)+1])
        final_data.loc[final_data['temp'].str.lower().str.contains(i), 'state'] = i.upper()
    """
    
    status = "Data Transformation Done..."
    print(status)
except:
    status = "Data Transformation error"
    print(status)
    flag = 0
    print("Full code run status:" ,flag, "\nfb connection status:", fb_connection  ,"\ndata_present status",data_present, "\nmysql connection status:" , connection_db)

"""final_data.loc[final_data['city'].str.lower().str.contains('nc|n.c|north carolina'), 'state'] = 'NC'
final_data.loc[final_data['city'].str.lower().str.contains('SC|sc'), 'state'] = 'SC'
final_data.loc[final_data['city'].str.lower().str.contains('ga|GA'), 'state'] = 'GA'"""


import datetime
from dateutil import parser
from dateutil import tz
from_zone = tz.tzutc()

one_hour_before_time = (datetime.datetime.now().replace(microsecond=0,second=0,minute=0) - datetime.timedelta(hours = 1)).replace(tzinfo=from_zone)
this_hour_time = (datetime.datetime.now().replace(microsecond=0,second=0,minute=0)).replace(tzinfo=from_zone)

final_data['Create_time'] = final_data['Create_time'].map(lambda x: parser.parse(x))
#final = final_data[final_data['Create_time'] > one_hour_before_time]

final = final_data[(final_data['Create_time'] >= one_hour_before_time) & (final_data['Create_time'] <= this_hour_time)]


#final['id'] = final['id'].astype(str).astype(float)
#final['first_name'] = final['first_name'].astype(str)

#final.dtypes
#final[["city", "state"]] = final["city"].apply(lambda x :str(x).split(",")[-1]).str.split("/",expand=True)

#split = final["city"].str.split(" ", expand = True) 

##final["city_mentioned"] = final["city"].str.split(" ", expand = True)[0]
#final["state_mentioned"] = 

#final["city"].str.split(" ", expand = True)[len(split.columns)-1]  if 'GA' is in ['GA','FL']
#mylist =['GA', 'fl', 'FL']
"""pattern = '|'.join(mylist)
if final["city"].str.split(" ", expand = True)[len(split.columns)-3].str.contains(pattern) == 'True':
    final["state_mentioned"] = final["city"].str.split(" ", expand = True)[len(split.columns)-1]

pd.DataFrame(list(final["city"].str.split(',').apply(lambda x: x[-1]).str.split('/')), columns=['city', 'state'])"""

try:
    if len(final) <= 0:
        status = "No new data ......."
        print(status)
        data_present = 0
        
    else:
        data_present = 1
        final['Platform'] = "Facebook"
        final['Source'] = "Internet"
        final['Lead_owner'] = ""
        final['Country'] = "USA"

        #final = final[['Create_time','id','Platform', 'Source']]
        
        #final.to_csv("FB_all_leads_data.csv")
    
    #### MUSQL DATA BASE CONNECTION 
        
        try:
            import mysql.connector
            mydb = mysql.connector.connect(
              host="localhost",
              user="root",
              passwd="Meritus@123",
              database="eug"
            )
            status = "Database connections made ......."
            print(status)
            connection_db = 1
        except:
            connection_db = 0
            flag = 0
            status = "Database connections not made ......."
            print(status)
            sys.exit()
        #print(mydb) 
        
        mycursor = mydb.cursor()
        
        """mycursor.execute("SHOW TABLES")
        for x in mycursor:
             print(x)"""
    
        all_val = []
        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "INSERT INTO facebook_leads_data (firstName, lastName,id,emailID,mobile,city,leadSource3ID,leadSource2ID,leadOwnerID,country,stateID,fb_creation_time,dB_creation_time,basedOnAssignmentRule) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)"
        for i in range(len(final)): 
            val = (final.iloc[i]['first_name'], final.iloc[i]['last_name'],final.iloc[i]['id'],final.iloc[i]['email'],
                   final.iloc[i]['Phone_number'],
                   final.iloc[i]['city'],final.iloc[i]['Platform'],final.iloc[i]['Source'],final.iloc[i]['Lead_owner'],
                   final.iloc[i]['Country'],final.iloc[i]['state'],final.iloc[i]['Create_time'].strftime('%Y-%m-%d %H:%M:%S'),time_now,"Yes")
            all_val.append(val)
        mycursor.executemany(sql, all_val)
        
        sql2 = """delete f1 from facebook_leads_data f1
        inner join facebook_leads_data f2 
        where  f1.pk_id > f2.pk_id and f1.`id` = f2.`id` and f1.fb_creation_time = f2.fb_creation_time;"""
    
        mydb.commit()
        
        mycursor2 = mydb.cursor()
        mycursor2.execute(sql2)
        mydb.commit()
        print(mycursor.rowcount, "record inserted.")
        print(mycursor2.rowcount, "Duplicates_deleted....")
        
    
    time2 = time.time()
    print("time taken for code to run is " + str(int(time2 - time1)) + " seconds" )
    flag = 1
except:
    flag = 0
    

#print("Full code run status:" ,flag, "\nfb connection status:", fb_connection  ,"\ndata_present status",data_present, "\nmysql connection status:" , connection_db)

print(data_present)

print(flag)