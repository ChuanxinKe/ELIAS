import pandas as pd
import sqlite3
import TerDec as td
import sqlite3
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize.casual import TweetTokenizer
from nltk.corpus import wordnet
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#################################

mission0=td.Mission('Initial')
words_list = pd.read_csv(r'keywords.csv')
b="select * from raw_tweets where tweets like '%elearning%'"
for index,row in words_list.iterrows():
    a=" or tweets like '%{}%'".format(row['keywords'])
    b=b+a
print(b)
conn = sqlite3.connect('elearning_tweets.db')
c = conn.cursor()
conn2=sqlite3.connect('elearning_tweets0606.db')
c2=conn2.cursor()
conn3=sqlite3.connect('cleaned_text.db')
c3=conn3.cursor()
process="CREATE TABLE IF NOT EXISTS tweets_process (id_str TEXT, tweets TEXT,user_id_str TEXT, timestamp_ms REAL,\
    user_location TEXT,user_followers REAL,user_friends REAL,user_favourites REAL,user_statuses REAL,tweet_clean TEXT,compound_tweet REAL)"
c3.execute(process)
conn3.commit()
update="INSERT INTO tweets_process (id_str, tweets,user_id_str,timestamp_ms,\
            user_location,user_followers,user_friends,user_favourites,user_statuses,tweet_clean,compound_tweet) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
mission0.end()

mission1=td.Mission('Initial stop words list')
# The complex of tweets needs special words list. We drop punctuations and tokentize before stop words.
# So the im, dont, youre needed. If drop stop words first, there are still other problems.
stop_words = set(stopwords.words('english'))
special_stopwords=['rt','amp','im','u','could','dont','youre','would','weve','cant'\
    'elearning','learning','teaching','education','learn','teach','online',\
    'remote','distance','digital'] 
stop_words.update(special_stopwords)
mission1.end()

mission2=td.Mission("Select data from two database")
df = pd.read_sql(b, conn)
df2 = pd.read_sql(b, conn2)
df=df.append(df2,sort=False)
df=df[~ df['tweets'].str.contains('#machinelearning|#MachineLearning|#Machinelearning|#MACHINELEARNING|machineLearning')]
df.drop_duplicates(subset='tweets', inplace=True)
#df.loc[:,'timestamp_ms'] = pd.to_datetime(df['timestamp_ms'],unit='ms')
print('Check Data Structure and first 5:\n')
df.info()
print('\n',df.head())
mission2.end()

mission3=td.Mission('Setup text processing functions')
tokenizer=TweetTokenizer()
wnl = WordNetLemmatizer()
analyzer = SentimentIntensityAnalyzer()
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
mission3.end()

mission4=td.Mission('Text processes')
mission4_1=td.Mission('Drop emoji,lowcase')
df.loc[:,"tweet_clean"] = df["tweets"]
df.loc[:,"tweet_clean"]  = [deEmojify(i) for i in df["tweet_clean"]]
df.loc[:,"tweet_clean"]  = [i.lower() for i in df["tweet_clean"]]

mission4_2=td.Mission('Drop url, retweet_at, # and punctuation')
df.loc[:,"tweet_clean"]   = [drop_at_url(i) for i in df["tweet_clean"]]
df.loc[:,"tweet_clean"] = [re.sub('[^a-zA-Z]', ' ',i) for i in df["tweet_clean"]]

mission4_3=td.Mission('Tokenize & Lemmatize')
df.loc[:,"tweet_clean"] = df["tweet_clean"].apply(lambda text: tokenizer.tokenize(text))
df.loc[:,"tweet_clean"] = [lemmatizeWords(i) for i in df["tweet_clean"]]
mission4_3.end()

mission4_4=td.Mission('Drop stop words')
df.loc[:,"tweet_clean"] = [drop_stop(i) for i in df["tweet_clean"]]

print('\nPrint cleaned Data:')
df.info()
print('\n',df.head())
mission4.end()

mission5=td.Mission('Get Vader sentiment intensity scores by average of sentence')
df.loc[:,"compound_tweet"] = [sen_sentiment(i) for i in df["tweets"]]
mission5.end()

mission6=td.Mission('submmit to database')
for index,row in df.iterrows():
    c3.execute(update,(row['id_str'],row['tweets'],row['user_id_str'],row['timestamp_ms'],row['user_location'],row['user_followers'],row['user_friends'],row['user_favourites'],row['user_statuses'],str(row['tweet_clean']),row['compound_tweet']))
    conn3.commit()

mission6.end()