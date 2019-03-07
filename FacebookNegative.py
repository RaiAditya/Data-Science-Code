
import requests
import pyodbc as db
import pandas as pd
import os
token = "token"

outputdf_CP = pd.DataFrame(columns=['Category','Date','Type','View_count'])

#connection = db.connect('Driver={SQL Server};''Server=RMWSQLADL01\DATAWHS;''Database=Marketing_Dashboard;''uid=RMW_SQL_Admin;pwd=Merilytics@123')
#cur = connection.cursor()

req = "page_id/insights/page_fans,page_fans_by_like_source,page_negative_feedback_by_type/day"

def req_facebook(req):
    r= requests.get("https://graph.facebook.com/v3.0/"+ req,{'access_token' : token})
    
    return r

def req_facebook1(req):
    a = requests.get(req)
    
    return a
#print(results)
data_list  = {}
def process(vals,name,types):
    external_links = []
    for val in vals :
        try:
            for key,value in zip(val['value'].keys(),val['value'].values()):    
                data = {}
                data[name]=key
                data['view_count'] = value
                data['date']=val['end_time'][:10]
                data['type'] = types
                external_links.append(data)
        except:
            pass
    return external_links
def process_views(vals,name):
    external_links = []
    try:
        for val in vals :  
            data = {}
            data[name]=val['value']
            data['date']=val['end_time']
            external_links.append(data)
    except:
        pass
    return external_links
def process_data(queue,limit,start):
    global results
    count=0
    while True:   
        result1 = results['data']
        if count>start:
            for rs in result1:
                if rs['name']=='page_negative_feedback_by_type':
                    vals = rs['values']
                    external_links=process(vals,'Category','Negative_Feedback')
                    if 'page_negative_feedback_by_type' not in data_list:
                        data_list['page_negative_feedback_by_type']=external_links
                    else:
                        data_list['page_negative_feedback_by_type']=data_list['page_negative_feedback_by_type']+external_links
                elif rs['name']=='page_fans_by_like_source':
                    vals = rs['values']
                    external_links=process(vals,'Category','Fans_by_Source')
                    if 'page_fans_by_like_source' not in data_list:
                        data_list['page_fans_by_like_source']=external_links
                    else:
                        data_list['page_fans_by_like_source']=data_list['page_fans_by_like_source']+external_links
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
    outputdf_CP = pd.DataFrame(columns=['Category','Date','Type','View_count'])
    for i,d in data.iterrows(): 
            sql = "insert into [dbo].[FB_Negative_int] values ("+','.join('\''+str(c)+'\'' for c in list(d))+")"
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
df.to_excel('Facebook_Negative.xlsx',index=False)
   
