import pandas as pd
import os

from textblob import TextBlob
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from subprocess import check_output
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tqdm import trange 
analyzer = SentimentIntensityAnalyzer()
import spacy
nlp = spacy.load('en_core_web_sm')

### adding all the smg sub files
os.chdir("/Users/21412/Documents/Pure Barre/smg/SMG Files/SMG Files")
for root, dirs, files in os.walk("."):
    df = pd.read_csv(files[0], sep='|')
    for filename in files[1:len(files)]:
        df1 = pd.read_csv(filename, sep='|')
        frames = [df, df1]
        df = pd.concat(frames,ignore_index=True)
        print(filename + " concatnated")

### Back to main directory
os.chdir("/Users/21412/Documents/Pure Barre/SMG_7_1")

df.to_csv("All New Smg Raw data.csv")

df['all_comments'] = df["S000032"].map(str) + df["S000033"].map(str) +df["S000034"].map(str) + df["S000041"].map(str)
df['teacher_comments'] = df["S000041"]

df.S000032 = df.S000032.fillna('')
df.S000033 = df.S000033.fillna('')
df.S000034 = df.S000034.fillna('')
df.S000041 = df.S000041.fillna('')


df["period"] = df["S000032"].map(str) + str(" ") + df["S000033"].map(str) + str(" ") + df["S000034"].map(str) + str(" ") + df["S000041"]

#define some parameters  
min_token_length = 2

def isNoise(token):     
    is_noise = False
    if token.is_stop == True:
        is_noise = True
    elif token.pos_ == 'PRON':
        is_noise = True
    elif token.pos_ == 'VERB' and token.shape_ == "'xx":
        is_noise = True
    elif len(token.string) <= min_token_length:
        is_noise = True
    elif str(token) == 'nan':
        is_noise = True
    elif token.is_punct == True:
        is_noise = True
    elif str(token) in ('she','She','class', 'makes','classes','he','makes','make',"n't","'ve",'The'):
        is_noise = True
    return is_noise

df['neg'] = ''
for i in trange(0,len(df)):
    try:
        name = df.iloc[i][54]
        doc = nlp(name)
        vs = analyzer.polarity_scores(name)
        if vs['compound'] < 0:
            df.loc[i,'neg'] = name
        else:
            for sent in doc.sents:
                text = str(sent)
                vst = analyzer.polarity_scores(text)
                neg = ''
                if vst['compound'] < -0.4:
                    neg = neg + " " + str(text)
                #print(sent.text)
                df.loc[i,'neg'] = neg
    except:
        print(i)




df['clean_words'] = ''
for i in trange(0,len(df)):
    try:
        name = df.iloc[i][55]
        doc = nlp(name)
        tokens = [token.text for token in doc if not token.is_punct]
        final_words = ""
        for token in doc:
            if isNoise(token) == False:
                final_words = final_words + " " + str(token)
        df.loc[i,'clean_words'] = final_words
    except:
        print(i)

textt = []
syub = []
bobo = []
vss = []
comp = []

for i in trange(len(df)):
    text = df.iloc[i][54]
    analysis = TextBlob(text)
    pol = analysis.sentiment[0]
    sub = analysis.sentiment[1]
    vs = analyzer.polarity_scores(text)
    textt.append(text)
    syub.append(sub)
    bobo.append(pol)
    vss.append(vs)
    comp.append(vs['compound'])
    
df['polarity'] = bobo
df['subjectivity'] = syub
df['vedar_senti'] = vss
df['comp'] = comp

textt = []
syub = []
bobo = []
vss = []
comp = []

for i in range(len(df)):
    text = df.iloc[i][51]
    analysis = TextBlob(text)
    pol = analysis.sentiment[0]
    sub = analysis.sentiment[1]
    vs = analyzer.polarity_scores(text)
    textt.append(text)
    syub.append(sub)
    bobo.append(pol)
    vss.append(vs)
    comp.append(vs['compound'])
    
#df['polarity'] = bobo
df['Tea_subjectivity'] = syub
#df['vedar_senti'] = vss
df['Tea_comp'] = comp

df.to_csv("All_new_smg_sentiment.csv")

df = df[df['TERM'] == 'WebOK'].reset_index(drop=True)
#df2 = df1
#df = df1

#df44 = pd.read_excel("All_new_smg_sentiment.xlsx")
#df = df44[df44['TERM'] == 'WebOK']

df['store_number'] = df["StoreId"].str.slice(0, 4, 1)

data = df[df['comp'] != 0]

summary1 = data.groupby(['store_number'])['comp'].mean().reset_index()
summary12 = data.groupby(['store_number'])['comp'].count().reset_index()

data1 = df[df['Tea_comp'] != 0] 

summary2 = data1.groupby(['store_number'])['Tea_comp'].mean().reset_index()
summary22 = data1.groupby(['store_number'])['Tea_comp'].count().reset_index()

summary3 = df[df['comp'] < 0].groupby('store_number')['comp'].count().reset_index()
summary4 = df[df['Tea_comp'] < 0].groupby('store_number')['Tea_comp'].count().reset_index()

result = pd.merge(summary1, summary12, on='store_number',left_index=False, right_index=False)
result.columns = ['store_number', 'Senti', 'count']
result = pd.merge(result, summary2, on='store_number',how='left')
result = pd.merge(result, summary22, on='store_number',how='left')
result.columns = ['store_number', 'Senti', 'count','teach_senti','count']

result = pd.merge(result, summary3, on='store_number',how='left')
result = pd.merge(result, summary4, on='store_number',how='left')
result.columns = ['store_number', 'Senti', 'count','teach_senti','count','Negative_total','Negative_Teach']


result.to_csv("Summary3.csv")
result = result.fillna('')

df.to_csv("All_smg_sentiment_WEBok.csv")

for i in range(len(result)):
    try:
        store_number = result.iloc[i][0]
        filter_df = df[df['store_number'] == store_number]
        
        filter_df = filter_df.sort_values(['comp'],ascending = [1]).reset_index(drop=True)
        if len(filter_df) > 0 and filter_df.loc[0,'comp'] < -0.05:
            result.loc[i,'most_negative_sentence'] = filter_df.loc[0,'period']
        if len(filter_df) > 1 and filter_df.loc[0,'comp'] < -0.05:
            result.loc[i,'second_negative_sentence'] = filter_df.loc[1,'period']
        if len(filter_df) > 2 and filter_df.loc[2,'comp'] < -0.05:
            result.loc[i,'third_negative_sentence'] = filter_df.loc[2,'period']
        result.loc[i,'sentiment_value'] = filter_df.loc[filter_df['comp'].argmin(),'comp']
        keywords = pd.Series(' '.join(filter_df["clean_words"]).lower().split()).value_counts()[:25]
        result.loc[i,'keywords'] = str(list(keywords.index[0:24]))
    except:
        print(i)

     

result.to_csv("Most_negative_sentence_key3.csv")

#os.chdir("/Users/21412/Documents/Pure Barre/smg")

from dateutil.parser import parse

for i in range(len(df)):
    df.loc[i,'week']  = parse(df.loc[i,'Date_Time']).isocalendar()[1]

weekly_final = pd.DataFrame()
for i in range(len(result)):
    try:
        store_number = result.iloc[i][0]
        weekly_df = df[df['store_number'] == store_number]
        
        weekly_df1 = weekly_df.groupby(['store_number', 'week'])['comp'].mean().reset_index()
        
        weekly_df2 = weekly_df1.pivot(index='store_number', columns='week', values='comp')
        frames = [weekly_final, weekly_df2]
        weekly_final = pd.concat(frames)
    except:
        print(i)


weekly_final.to_csv("weekly_Sentiment_studio_wise3333.csv")

#df = pd.read_csv("PB_RawData_10152018.csv", sep='|', lineterminator='/r')