# -*- coding: utf-8 -*-
"""
Created on Tue Jul 24 14:42:12 2018

@author: 21329
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 11:43:24 2018

@author: 21329
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import requests
import pyodbc as db
import pandas as pd
import os
token = "token"

outputdf_CP = pd.DataFrame(columns=['Category','Date','Fans_Count','Type'])

#connection = db.connect('Driver={SQL Server};''Server=RMWSQLADL01\DATAWHS;''Database=Marketing_Dashboard;''uid=RMW_SQL_Admin;pwd=Merilytics@123')
#cur = connection.cursor()

req = "page_id/insights/page_views_total"

def req_facebook(req):
    r= requests.get("https://graph.facebook.com/v3.0/"+ req,{'access_token' : token})
    return r

def req_facebook1(req):
    a = requests.get(req)
    return a
#print(results)

data_list  = {}
#all data in rows
def process(vals,name,types):
    external_links = []
    for val in vals :
        try:
            for key,value in zip(val['value'].keys(),val['value'].values()):    
                data = {}
                data[name]=key
                data['fans_count'] = value
                data['date']=val['end_time'][:10]
                data['type'] = types
                external_links.append(data)
        except:
            pass
    return external_links

#if only value and date
def process_views(vals,name):
    external_links = []
    try:
        for val in vals :  
            data = {}
            data[name]=val['value']
            data['date']=val['end_time'][:10]
            external_links.append(data)
    except:
        pass
    return external_links

"""
data = pd.DataFrame()
for i in range(len(results['data'])):
    des = results['data'][i]['description']
    idd = results['data'][i]['id']
    name = results['data'][i]['name']
    title = results['data'][i]['title']
    for j in range(len(results['data'][i]['values'])):
        tt = pd.DataFrame(results['data'][i]['values'][j], index=[0])
        tt['des'] = des
        tt['id'] = idd
        tt['name'] = name
        tt['title'] = title
        data = data.append(tt)
        
        
    
tt = pd.DataFrame(results['data'][0]['values'][0])

"""



def process_data(queue,limit,start):
    global results
    count=0
    while True:   
        result1 = results['data']
        if count>start:
            for rs in result1:
                if rs['name']=='page_fans_locale':
                    vals = rs['values']
                    external_links=process(vals,'Category','language')
                    if 'page_fans_locale' not in data_list:
                        data_list['page_fans_locale']=external_links
                    else:
                        data_list['page_fans_locale']=data_list['page_fans_locale']+external_links
                elif rs['name']=='page_fans_city':
                    vals = rs['values']
                    external_links=process(vals,'Category','City')
                    if 'page_fans_city' not in data_list:
                        data_list['page_fans_city']=external_links
                    else:
                        data_list['page_fans_city']=data_list['page_fans_city']+external_links
                elif rs['name']=='page_fans_country':
                    vals = rs['values']
                    external_links=process(vals,'Category','Country')
                    if 'page_fans_country' not in data_list:
                        data_list['page_fans_country']=external_links
                    else:
                        data_list['page_fans_country']=data_list['page_fans_country']+external_links
                elif rs['name']=='page_fans_gender_age':
                    vals = rs['values']
                    external_links=process(vals,'Category','Gender')
                    if 'page_fans_gender_age' not in data_list:
                        data_list['page_fans_gender_age']=external_links
                    else :
                        data_list['page_fans_gender_age']=data_list['page_fans_gender_age']+external_links                       
            #print(count,limit,start)
        if count==limit:
            break
        if queue in results['paging']:
            results=req_facebook1(results['paging'][queue]).json()
            count = count+1
        else:
            break
    return count

def open_json(data):
    outputdf_CP = pd.DataFrame(columns=['Category','Date','Fans_Count','Type'])
    for i,d in data.iterrows(): 
            sql = "insert into [dbo].[FB_Fans_int] values ("+','.join('\''+str(c)+'\'' for c in list(d))+")"
            #print(sql)
            cur.execute(sql)
            cur.commit()

results = req_facebook(req).json()  
ct = process_data('next',4,-1)
process_data('previous',3,ct)
    

Vals = []
for key,value in zip(data_list.keys(),data_list.values()):
    Vals+=value 
    try:
        key = key[-31:]
    except:
        pass
df = pd.DataFrame(Vals)
open_json(df)
df.to_excel('FacebookFans.xlsx',index=False)
