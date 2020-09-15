import pandas as pd
import TerDec as td
import sqlite3
from pandas.io import sql
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize.casual import TweetTokenizer
from nltk.corpus import wordnet
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#################################

mission0=td.Mission('Initial')
post_select=r'select * from submission INNER JOIN (select DISTINCT id as id2,tfmark from in_keywords where tfmark="t") as a on submission.id=a.id2 where num_comment>0 AND body <> "[removed]" '
comment_select=r'select * from comment INNER JOIN (select id as id2 from submission INNER JOIN (select DISTINCT id as id3, tfmark from in_keywords where tfmark="t") as a on submission.id=a.id3 where num_comment>0 AND body <> "[removed]") as b on comment.id=b.id2 where comment.comment <> "[刪除]" and comment.comment <> "[已移除]"'
submission_process=r'CREATE TABLE IF NOT EXISTS submission_process (id TEXT, time TEXT,subreddit TEXT, subreddit_id TEXT, num_comment INTEGER,title TEXT, body TEXT,tfmark TEXT, title_clean TEXT,body_clean TEXT,compound_title REAL,compound_body REAL)'
comment_process=r'CREATE TABLE IF NOT EXISTS comment_process (id TEXT,comment TEXT,comment_clean TEXT,compound_comment REAL)'
update_submission=r'INSERT INTO submission_process (id, time,subreddit, subreddit_id, num_comment,title, body,tfmark, title_clean,body_clean,compound_title,compound_body) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
update_comment=r'INSERT INTO comment_process (id,comment,comment_clean,compound_comment) VALUES (?,?,?,?)'
conn = sqlite3.connect('cleaned_text.db')
c = conn.cursor()
c.execute(submission_process)
c.execute(comment_process)
conn.commit()
mission0.end()

mission1=td.Mission('Initial stop words list')
# The complex of tweets needs special words list. We drop punctuations and tokentize before stop words.
# So the im, dont, youre needed. If drop stop words first, there are still other problems.
stop_words = set(stopwords.words('english'))
special_stopwords=['rt','amp','im','u','could','dont','youre','would','weve','cant'\
    'elearning','learning','teaching','education','learn','teach','online',\
    'remote','distance','digital'] 
stop_words.update(special_stopwords)
for j in 'qwertyuiopasdfghjklzxcvbnm':
    stop_words.update(j)
mission1.end()

mission2=td.Mission('Setup text processing functions')
tokenizer=TweetTokenizer()
wnl = WordNetLemmatizer()
analyzer = SentimentIntensityAnalyzer()

def sen_sentiment(paragraph):
    sentence_list = nltk.tokenize.sent_tokenize(paragraph)
    paragraphSentiments = 0.0
    if len(sentence_list)==0:
        pass
    else:
        for sentence in sentence_list:
            vs = analyzer.polarity_scores(sentence)
            paragraphSentiments += vs["compound"]
        return round(paragraphSentiments / len(sentence_list), 4)

def data_process(df,target,clean):
    def deEmojify(inputString): 
        return inputString.encode('ascii', 'ignore').decode('ascii')
    def drop_at_url(inputString):
        #From string to list, after processing back to string. Not efficient, but work and fit other processes
        cached=inputString.split()
        new=[]
        for i in cached:
            if "@" in i or r"http:/" in i or r"https:/" in i or '#' in i:
                pass
            else:
                new.append(i)
        outstring=" ".join(new)
        return outstring
    def drop_stop(lists):
        #You can add stop words by the golbal variable, stop_words 
        new=[]
        global stop_words
        for i in lists:
            if i not in stop_words:
                new.append(i)
        return new
    def lemmatizeWords(lists):
        #Map POS tag to first character lemmatize() accepts
        new=[]
        for i in lists:
            tag = nltk.pos_tag([i])[0][1][0].upper()
            tag_dict = {"J": wordnet.ADJ,
                    "N": wordnet.NOUN,
                    "V": wordnet.VERB,
                    "R": wordnet.ADV}       
            pos=tag_dict.get(tag, wordnet.NOUN)
            new.append(wnl.lemmatize(i,pos))      
        return new

    #Drop emoji, lowcase, drop url, @#, punctuation')
    df.loc[:,clean] = df[target]
    df.loc[:,clean]  = [deEmojify(i) for i in df[clean]]
    df.loc[:,clean]  = [i.lower() for i in df[clean]]
    df.loc[:,clean]   = [drop_at_url(i) for i in df[clean]]
    df.loc[:,clean] = [re.sub('[^a-zA-Z]', ' ',i) for i in df[clean]]
    #Tokenize and Lemmatize')
    df.loc[:,clean] = df[clean].apply(lambda text: tokenizer.tokenize(text))
    df.loc[:,clean] = [lemmatizeWords(i) for i in df[clean]]
    #Drop stop words')
    df.loc[:,clean] = [drop_stop(i) for i in df[clean]]
mission2.end()

mission3=td.Mission("Select data from each file")
ct=td.counter(description='DB files done: ')
for i in range(2,21):
    start_num=i*5
    end_num=(i+1)*5
    database="elearning_reddit{}-{}.db".format(start_num,end_num)
    conn_tamp = sqlite3.connect(database)
    c_tamp = conn_tamp.cursor()
    df_s = pd.read_sql(post_select, conn_tamp)
    del df_s['id2']
    df_c = pd.read_sql(comment_select, conn_tamp)
    del df_c['id2']

    mission4=td.Mission('Text Process')
    data_process(df_s,'title','title_clean')
    data_process(df_s,'body','body_clean')
    data_process(df_c,'comment','comment_clean')
    mission4.end()

    mission5=td.Mission('Get Vader sentiment intensity scores by average of sentence')
    df_s.loc[:,"compound_title"] = [sen_sentiment(i) for i in df_s["title"]]
    df_s.loc[:,"compound_body"] = [sen_sentiment(i) for i in df_s["body"]]
    df_c.loc[:,"compound_comment"] = [sen_sentiment(i) for i in df_c["comment"]]
    mission5.end()

    mission6=td.Mission('submmit to database')
    for index,row in df_s.iterrows():
        c.execute(update_submission,(row['id'],row['time'],row['subreddit'],row['subreddit_id'],row['num_comment'],row['title'],row['body'],row['tfmark'],str(row['title_clean']),str(row['body_clean']),row['compound_title'],row['compound_body']))
        conn.commit()

    for index,row in df_c.iterrows():
        c.execute(update_comment,(row['id'],row['comment'],str(row['comment_clean']),row['compound_comment']))
        conn.commit()
    ct.flush()
    mission6.end()