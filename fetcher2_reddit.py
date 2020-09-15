import requests
import datetime as dt
import sqlite3
import TerDec as td
from langdetect import detect,DetectorFactory
import praw
import pandas as pd
#################
mission0=td.Mission('Set Parameters')
in_a=int(input('start time: '))
in_b=int(input('end time: '))
in_c=int(input('day after 0715: '))
database_name='elearning_reddit{}-{}.db'.format(in_a,in_b)
keywords=[]
words_list = pd.read_csv(r'keywords.csv')
for index,row in words_list.iterrows():
    keywords.append(row['keywords'])

reddit = praw.Reddit(client_id='', \
                     client_secret='', \
                     user_agent='', \
                     username='', \
                     password='')

table_id="CREATE TABLE IF NOT EXISTS in_keywords (id TEXT, contents TEXT, time TEXT, tbmark TEXT,keywords TEXT, tfmark TEXT,lang TEXT)"
upload_id="INSERT INTO in_keywords (id,contents,time,tbmark,keywords,tfmark,lang) VALUES (?,?,?,?,?,?,?)"
table_idlog="CREATE TABLE IF NOT EXISTS idlog (time TEXT, return INTEGER, total INTEGER, keywords TEXT, start_date TEXT, end_date TEXT,tbmark TEXT)"
upload_idlog="INSERT INTO idlog (time,return,total,keywords,start_date,end_date,tbmark) VALUES (?,?,?,?,?,?,?)"
table_sub="CREATE TABLE IF NOT EXISTS submission (id TEXT, time TEXT,subreddit TEXT, subreddit_id TEXT, num_comment INTEGER,title TEXT, body TEXT)"
upload_sub="INSERT INTO submission (id, time,subreddit, subreddit_id, num_comment,title, body) VALUES (?,?,?,?,?,?,?)"
table_comm="CREATE TABLE IF NOT EXISTS comment (id TEXT,comment TEXT)"
upload_comm="INSERT INTO comment (id,comment) VALUES (?,?)"
table_cmlog="CREATE TABLE IF NOT EXISTS cmlog (time_s TEXT,time_e TEXT,num INTEGER,wrong_s INTEGER,wrong_c INTEGER,c_done INTEGER)"
upload_cmlog="INSERT INTO cmlog (time_s,time_e,num,wrong_s,wrong_c,c_done) VALUES (?,?,?,?,?,?)"

conn = sqlite3.connect(database_name)
c = conn.cursor()
c.execute(table_id)
c.execute(table_idlog)
c.execute(table_sub)
c.execute(table_comm)
c.execute(table_comm)
c.execute(table_cmlog)

view_words=['time','tbmark','keywords','tfmark','lang']
for i in view_words:
    view_commit="CREATE VIEW IF NOT EXISTS {} AS select {},count(id) from in_keywords group by {} order by {}".format(i,i,i,i)
    c.execute(view_commit)
mission0.end()

mission1=td.Mission('Set Functions')

def time_trans(utc):
    return dt.datetime.utcfromtimestamp(utc).strftime('%d-%m-%Y')

def get_pushshift_data(data_type, **kwargs):
    try:
        base_url = f"https://api.pushshift.io/reddit/search/{data_type}/"
        payload = kwargs
        request = requests.get(base_url, params=payload)
        return request.json()
    except:
        pass

DetectorFactory.seed = 0
def get_s_id(jsondata,torb):
    def tbhandler(data):
        if torb=='t':
            return data['title']
        elif torb=='b':
            return data['selftext']
        else:
            return 'Wrong, only t or b are accepted!'
    
    try:
        time_now = dt.datetime.now()
        s_return=jsondata['metadata']['results_returned']
        s_results=jsondata['metadata']['total_results']
        s_start=time_trans(jsondata['metadata']['after'])
        s_end=time_trans(jsondata['metadata']['before'])
        s_keywords=tbhandler(jsondata['metadata'])
        c.execute(upload_idlog,(time_now,s_return,s_results,s_keywords,s_start,s_end,torb))
        conn.commit()
    except:
        c.execute(upload_idlog,('wrong',0,0,'wrong','wrong','wrong','wrong'))
        conn.commit()
     
    try:
        for i in jsondata['data']:
            s_id=i['id']
            s_content=tbhandler(i)
            s_time=time_trans(i['created_utc'])
            
            for j in keywords:
                if j in s_content.lower():
                    tfmark='t'
                    break
                else:
                    tfmark='f'
            lang=detect(s_content)
            c.execute(upload_id,(s_id,s_content,s_time,torb,s_keywords,tfmark,lang))
            conn.commit()
            ct1.flush()    
    except:
        pass
mission1.end()

mission2=td.Mission('Fetch from Keywords')
ct1=td.counter(description='Items into database',sleep=0)
for i in keywords:
    for j in range(in_a+in_c,in_b+in_c,1):
        title_data=get_pushshift_data(data_type="submission",metadata='True',after=str(j+1)+'d',before=str(j)+'d',title=i,size=500)
        get_s_id(title_data,'t')
        body_data=get_pushshift_data(data_type="submission",metadata='True',after=str(j+1)+'d',before=str(j)+'d',selftext=i,size=500)
        get_s_id(body_data,'b')
mission2.end()


mission3=td.Mission('Produce IDs')
df = pd.read_sql("SELECT * FROM in_keywords", conn)
df.drop_duplicates(subset='id', inplace=True)
print('Check Data Structure and first 5:\n')
df.info()
print('\n',df.head())
mission3.end()

mission4=td.Mission('Use ID list')
ct2=td.counter(description='IDs Done',sleep=0)
time_s=dt.datetime.now()
wrong_s=0
wrong_c=0
for index,row in df.iterrows():
    try:
        s_json=get_pushshift_data(data_type="submission",ids=row['id'])       
        s_data=s_json['data'][0]
        s_subreddit=s_data['subreddit']
        s_subreddit_id=s_data['subreddit_id']
        s_num_comm=s_data['num_comments']
        s_title=s_data['title']
        s_body=s_data['selftext']

        c.execute(upload_sub,(row['id'],row['time'],s_subreddit,s_subreddit_id,s_num_comm,s_title,s_body))
        conn.commit()
    except:
        wrong_s=wrong_s+1

    try:
        submission = reddit.submission(id=row['id'])
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            c.execute(upload_comm,(row['id'],comment.body))
            conn.commit()
        ct2.flush()
    except:
        wrong_c=wrong_c+1
    
time_e=dt.datetime.now()
num=df.shape[0]
c.execute(upload_cmlog,(time_s,time_e,num,wrong_s,wrong_c,ct2.number))
conn.commit()

mission4.end()