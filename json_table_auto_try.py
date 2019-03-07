from os import getenv
import requests
import pandas as pd
import pyodbc as db
import os
token = "EAAD4BZAl5iDABAA2U4fpmOvU35tmmxeELuGMmD6vrADVjh30PZClc0IJx7uRj3xuvE4xF4ZCNlzYVM7jRETeant0FFETpPpFZAZCQR1QXDNKvxTonShZBuSQuNXIAYUBe7T0jQrFvWnC6Yt4aloIF272qRYOjIPZAVg8Evp4SbIx6uFqBuF6WzGxZA9LBjRczoMZD"

#results = r.json()
#connection = db.connect('Driver={SQL Server};''Server=RMWSQLADL01\DATAWHS;''Database=Marketing_Dashboard;''uid=RMW_SQL_Admin;pwd=Merilytics@123')
#cur = connection.cursor()

outputdf_CP = pd.DataFrame(columns=['Category','Date','Types','View_count'])

req = "301959993192748/insights/page_fans,page_views_by_profile_tab_logged_in_unique,page_views_by_age_gender_logged_in_unique,page_views_by_site_logged_in_unique,page_views_external_referrals,page_content_activity_by_city_unique"

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
                data['types'] = types
                external_links.append(data)
        except:
            pass
    return external_links
def process_views(vals,name,types):
    external_links = []
    try:
        for val in vals :  
            data = {}
            data[name]='fan_count'
            data['view_count']=val['value']
            data['date']=val['end_time'][:10]
            data['types'] = types
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
                if rs['name']=='page_views_external_referrals' and rs['period'] == 'day':
                    vals = rs['values']
                    external_links=process(vals,'Category','links')
                    if 'page_views_external_referrals' not in data_list:
                        data_list['page_views_external_referrals']=external_links
                    else:
                        data_list['page_views_external_referrals']=data_list['page_views_external_referrals']+external_links
                elif rs['name']=='page_content_activity_by_city_unique' and rs['period'] == 'week':
                    vals = rs['values']
                    external_links=process(vals,'Category','city')
                    if 'page_content_activity_by_city_unique' not in data_list:
                        data_list['page_content_activity_by_city_unique']=external_links
                    else:
                        data_list['page_content_activity_by_city_unique']=data_list['page_content_activity_by_city_unique']+external_links
                elif rs['name']=='page_views_by_profile_tab_logged_in_unique' and rs['period'] == 'day':
                    vals = rs['values']
                    external_links=process(vals,'Category','tab_name')
                    if 'page_views_by_profile_tab_logged_in_unique' not in data_list:
                        data_list['page_views_by_profile_tab_logged_in_unique']=external_links
                    else :
                        data_list['page_views_by_profile_tab_logged_in_unique']=data_list['page_views_by_profile_tab_logged_in_unique']+external_links
                elif rs['name']=='page_views_by_site_logged_in_unique' and rs['period'] == 'day':
                    vals = rs['values']
                    external_links=process(vals,'Category','device')
                    if 'page_views_by_site_logged_in_unique' not in data_list:
                        data_list['page_views_by_site_logged_in_unique']=external_links
                    else:
                        data_list['page_views_by_site_logged_in_unique'] = data_list['page_views_by_site_logged_in_unique']+external_links
                elif rs['name']=='page_views_by_age_gender_logged_in_unique' and rs['period'] == 'day':
                    vals = rs['values']
                    external_links=process(vals,'Category','gender')
                    if 'page_views_by_age_gender_logged_in_unique' not in data_list:
                        data_list['page_views_by_age_gender_logged_in_unique']=external_links
                    else:
                        data_list['page_views_by_age_gender_logged_in_unique']=data_list['page_views_by_age_gender_logged_in_unique']+external_links
                elif rs['name']=='page_fans':
                    vals = rs['values']
                    external_links=process_views(vals,'Category','Page_fans')
                    if 'page_fans' not in data_list:
                        data_list['page_fans']=external_links
                    else:
                        data_list['page_fans']=data_list['page_fans']+external_links
            #print(count,limit,start)
        if count==limit:
            break
        if queue in results['paging']:
            results=req_facebook1(results['paging'][queue]).json()
            count = count+1
        else:
            break
    return count

results = req_facebook(req).json() 
ct = process_data('next',4,-1)

Vals = []
for key,value in zip(data_list.keys(),data_list.values()):
    Vals+=value 
    try:
        key = key[-31:]
    except:
        pass
df = pd.DataFrame(Vals)

data = pd.DataFrame()
for i in range(len(results['data'])):
    des = results['data'][i]['description']
    idd = results['data'][i]['id']
    name = results['data'][i]['name']
    title = results['data'][i]['title']
    for j in range(len(results['data'][i]['values'])):
        if results['data'][i]['values'][j]['value'] == {} :
            tt = pd.DataFrame(results['data'][i]['values'][j]['value'], index=[0])
            tt = tt.transpose()
            tt['category'] = tt.index
            tt.columns = ['value', 'category']
            tt['des'] = des
            tt['id'] = idd
            tt['name'] = name
            tt['title'] = title
            tt['date'] = results['data'][i]['values'][1]['end_time']
            data = data.append(tt)
        try:
            if results['data'][i]['values'][j]['value'] != {} and not str(results['data'][i]['values'][j]['value']).isnumeric() :
                tt = pd.DataFrame(results['data'][i]['values'][j]['value'], index=[0])
                tt = tt.transpose()
                tt['category'] = tt.index
                tt.columns = ['value', 'category']
                #tt['category'] = "no_category"
                tt['des'] = des
                tt['id'] = idd
                tt['name'] = name
                tt['title'] = title
                tt['date'] = results['data'][i]['values'][j]['end_time']
                #tt.columns = ['date', 'value', 'category', 'des', 'id', 'name', 'title']
                tt = tt[['value','category','des','id','name','title','date']]
                data = data.append(tt)
        except:
            print(i,j)
        else:
            tt = pd.DataFrame(results['data'][i]['values'])
            tt['category'] = "no_category"
            tt['des'] = des
            tt['id'] = idd
            tt['name'] = name
            tt['title'] = title
            tt.columns = ['date', 'value', 'category', 'des', 'id', 'name', 'title']
            tt = tt[['value','category','des','id','name','title','date']]
            data = data.append(tt)

