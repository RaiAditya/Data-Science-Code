from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import random 
import pandas as pd
from bs4 import BeautifulSoup
full_data = pd.DataFrame()
browser = webdriver.Firefox()
data = pd.read_excel('Pure Barre Facebook links.xlsx')
data1 = pd.read_excel('data3.xlsx')
review_link = '/reviews/?ref=page_internal'
not_data = []
count = 0

for i in range(1,100):
    link = data1.iloc[i][0]
    url_main = link + review_link
    
    try:
        browser.get(link)
        time.sleep(random.randint(3,8))
        b = browser.page_source
        soup = BeautifulSoup(b,'html.parser')
        
        a = soup.find_all("div",{"class":"_4-u2 _6590 _3xaf _4-u8"})
        people_like = [x.text for x in a]
        
        #button = browser.find_elements_by_class_name('_2yaa')[4]
        #button.click()
        
        browser.get(url_main)
        time.sleep(random.randint(3,8))

        
        elem = browser.find_element_by_tag_name("body")
        time.sleep(random.randint(1,2))
        no_of_pagedowns = 10
        
        while no_of_pagedowns:
            elem.send_keys(Keys.PAGE_DOWN)
            time.sleep(random.randint(1,2))
            no_of_pagedowns-=1
            
        time.sleep(random.randint(2,5))
        a=browser.page_source
        soup=BeautifulSoup(a,'html.parser')
        #soup.prettify()
        review = soup.find_all("div",{"class":"_5pbx userContent _3576"})
        reviews = [x.text for x in review]
        
        date = soup.find_all("span",{"class":"timestampContent"})
        dates = [x.text for x in date]
        
        rating = soup.find_all("i",{"class":"_51mq img sp_3hEPNggZn1K sx_1a8fc0"})
        ratings = [x.text for x in rating]
        time.sleep(random.randint(1,3))
        
        if (len(ratings) - len(reviews) > 0 ):
            ratings = ratings[0:len(reviews)]
            dates = dates[0:len(reviews)]

        try:
            dataf = pd.DataFrame(
            {
             'link':link ,
             'like_and_foloower': people_like[0],
             'revie': reviews,
             'rating':ratings,
             'dates': dates,
            })
        except:
            dataf = pd.DataFrame(
            {
             'link':link ,
             'like_and_foloower': people_like[0],
             'revie': reviews,
             'rating':'5',
             'dates': dates,
            })
        #time.sleep(3)
            print("not_data")
            count = count + 1
            
        time.sleep(random.randint(5,8))
        full_data = full_data.append(dataf)
        full_data.to_csv("PB_sample1010_v2231.csv")
        print(i + 700)
    except:
        print(i)    
        not_data.append(i)
        time.sleep(random.randint(2,4))
    full_data.to_csv("PB_sample1010_v2231.csv")
