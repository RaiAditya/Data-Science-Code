
import requests
import pyodbc as db
import pandas as pd
import os
#Permenant Access Token
token = "token"

outputdf_CP = pd.DataFrame(columns=['Category','Date','Type','Value'])

#for database connection
#connection = db.connect('Driver={SQL Server};''Server=RMWSQLADL01\DATAWHS;''Database=Marketing_Dashboard;''uid=RMW_SQL_Admin;pwd=Merilytics@123')
#cur = connection.cursor()

#Include required metrics 
req = "Page_id/insights/page_total_actions,page_views_total,page_views_logged_in_unique,page_fan_removes,page_fan_adds,page_fans_by_like_source,page_impressions_unique,page_impressions_paid_unique,page_impressions_organic_unique,page_actions_post_reactions_like_total,page_actions_post_reactions_love_total,page_actions_post_reactions_wow_total,page_actions_post_reactions_haha_total,page_actions_post_reactions_sorry_total,page_actions_post_reactions_anger_total/day"

#call to facebook API
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
def process_views(vals,types,Category):
    external_links = []
    try:
        for val in vals :  
            data = {}
            data['value']=val['value']
            data['date']=val['end_time'][:10]
            data['Category'] = Category
            data['type'] = types
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
                if rs['name']=='page_views_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Views','Views')
                    if 'page_views_total' not in data_list:
                        data_list['page_views_total']=external_links
                    else:
                        data_list['page_views_total']=data_list['page_views_total']+external_links
                        
                elif rs['name']=='page_views_logged_in_unique':
                    vals = rs['values']
                    external_links=process_views(vals,'Views','U_Views')
                    if 'page_views_logged_in_unique' not in data_list:
                        data_list['page_views_logged_in_unique']=external_links
                    else:
                        data_list['page_views_logged_in_unique']=data_list['page_views_logged_in_unique']+external_links
                        
                elif rs['name']=='page_actions_post_reactions_like_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Reactions','Like')
                    if 'page_actions_post_reactions_like_total' not in data_list:
                        data_list['page_actions_post_reactions_like_total']=external_links
                    else:
                        data_list['page_actions_post_reactions_like_total']=data_list['page_actions_post_reactions_like_total']+external_links
                        
                elif rs['name']=='page_actions_post_reactions_love_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Reactions','Love')
                    if 'page_actions_post_reactions_love_total' not in data_list:
                        data_list['page_actions_post_reactions_love_total']=external_links
                    else :
                        data_list['page_actions_post_reactions_love_total']=data_list['page_actions_post_reactions_love_total']+external_links
                        
                elif rs['name']=='page_actions_post_reactions_wow_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Reactions','wow')
                    if 'page_actions_post_reactions_wow_total' not in data_list:
                        data_list['page_actions_post_reactions_wow_total']=external_links
                    else:
                        data_list['page_actions_post_reactions_wow_total']=data_list['page_actions_post_reactions_wow_total']+external_links
                        
                elif rs['name']=='page_actions_post_reactions_haha_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Reactions','haha')
                    if 'page_actions_post_reactions_haha_total' not in data_list:
                        data_list['page_actions_post_reactions_haha_total']=external_links
                    else:
                        data_list['page_actions_post_reactions_haha_total']=data_list['page_actions_post_reactions_haha_total']+external_links
                        
                elif rs['name']=='page_actions_post_reactions_sorry_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Reactions','sorry')
                    if 'page_actions_post_reactions_sorry_total' not in data_list:
                        data_list['page_actions_post_reactions_sorry_total']=external_links
                    else:
                        data_list['page_actions_post_reactions_sorry_total']=data_list['page_actions_post_reactions_sorry_total']+external_links
                        
                elif rs['name']=='page_actions_post_reactions_anger_total':
                    vals = rs['values']
                    external_links=process_views(vals,'Reactions','anger')
                    if 'page_actions_post_reactions_anger_total' not in data_list:
                        data_list['page_actions_post_reactions_anger_total']=external_links
                    else:
                        data_list['page_actions_post_reactions_anger_total']=data_list['page_actions_post_reactions_anger_total']+external_links                        
                elif rs['name']=='page_fan_removes':
                    vals = rs['values']
                    external_links=process_views(vals,'Likes','Unlikes')
                    if 'page_fan_removes' not in data_list:
                        data_list['page_fan_removes']=external_links
                    else:
                        data_list['page_fan_removes']=data_list['page_fan_removes']+external_links
                        
                elif rs['name']=='page_fan_adds':
                    vals = rs['values']
                    external_links=process_views(vals,'Likes','Organic_Likes')
                    if 'page_fan_adds' not in data_list:
                        data_list['page_fan_adds']=external_links
                    else:
                        data_list['page_fan_adds']=data_list['page_fan_adds']+external_links
                        
                elif rs['name']=='page_impressions_unique':
                    vals = rs['values']
                    external_links=process_views(vals,'Reach','Total_Reach')
                    if 'page_impressions_unique' not in data_list:
                        data_list['page_impressions_unique']=external_links
                    else:
                        data_list['page_impressions_unique']=data_list['page_impressions_unique']+external_links
                        
                elif rs['name']=='page_impressions_paid_unique':
                    vals = rs['values']
                    external_links=process_views(vals,'Reach','Paid_Reach')
                    if 'page_impressions_paid_unique' not in data_list:
                        data_list['page_impressions_paid_unique']=external_links
                    else:
                        data_list['page_impressions_paid_unique']=data_list['page_impressions_paid_unique']+external_links
                        
                elif rs['name']=='page_impressions_organic_unique':
                    vals = rs['values']
                    external_links=process_views(vals,'Reach','Organic_Reach')
                    if 'page_impressions_organic_unique' not in data_list:
                        data_list['page_impressions_organic_unique']=external_links
                    else:
                        data_list['page_impressions_organic_unique']=data_list['page_impressions_organic_unique']+external_links
                elif rs['name']=='page_total_actions':
                    vals = rs['values']
                    external_links=process_views(vals,'CTA','CTA')
                    if 'page_total_actions' not in data_list:
                        data_list['page_total_actions']=external_links
                    else:
                        data_list['page_total_actions']=data_list['page_total_actions']+external_links
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
    outputdf_CP = pd.DataFrame(columns=['Category','Date','Type','Value'])
    for i,d in data.iterrows(): 
            sql = "insert into [dbo].[FB_Data_int] values ("+','.join('\''+str(c)+'\'' for c in list(d))+")"
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
#open_json(df)
df.to_excel('Facebookdata.xlsx',index=False)
