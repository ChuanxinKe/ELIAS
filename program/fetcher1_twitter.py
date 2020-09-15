#LIBRARIES
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
import datetime
import time
import TerDec as td # Personal module,needs TerDec.py
############################

mission0_1=td.Mission('Initial variables. Connecting to database') #Mission Reply tool from TerDec.py
ct=td.counter(description='Tweets into database',sleep=0) #Counter from TerDec.py
dbpath=td.setpath(r'eLearning_tweets.db') #Path tool from TerDec.py
dbpath.askupdate('Path of database')
conn = sqlite3.connect(dbpath.path)
c = conn.cursor()
today_table="CREATE TABLE IF NOT EXISTS raw_tweets (id_str TEXT, tweets TEXT,user_id_str TEXT, timestamp_ms REAL,\
    user_location TEXT,user_followers REAL,user_friends REAL,user_favourites REAL,user_statuses REAL)"
today_upload="INSERT INTO raw_tweets (id_str, tweets,user_id_str, timestamp_ms,\
              user_location,user_followers,user_friends,user_favourites,user_statuses) VALUES (?,?,?,?,?,?,?,?,?)"
c.execute(today_table)
conn.commit()
mission0_1.end()

mission0_2=td.Mission('Get API Keys')
ckey=""
csecret=""
atoken=""
asecret=""
mission0_2.end()

# class object
class listener(StreamListener): #listener is being declared as a class inheriting from base class StreamListener

    def fulltext_handler(self,jsondata):
        try:
            full=jsondata['retweeted_status']['extended_tweet']['full_text']
            return full
        except:
            try:
                full=jsondata['extended_tweet']['full_text']
                return full
            except:
                full=jsondata['text']
                return full
   
    def on_data(self, data): #this is therefore a method from StreamListener
        try: 
            raw_data = json.loads(data)
            lang=raw_data['lang']
            if lang=='en':    # first filter by this label, keep it into database for check
                tweets=self.fulltext_handler(raw_data)
                id_str=raw_data['id_str']
                user_id_str=raw_data['user']['id_str']
                user_location=raw_data['user']['location']
                user_followers=raw_data['user']['followers_count']
                user_friends=raw_data['user']['friends_count']
                user_favourites=raw_data['user']['favourites_count']
                user_statuses=raw_data['user']['statuses_count']
                timestamp_ms=raw_data['timestamp_ms']
                c.execute(today_upload,(id_str,tweets,user_id_str,timestamp_ms,\
                    user_location,user_followers,user_friends,user_favourites,user_statuses))
                conn.commit()
                ct.flush() # Show number into database
            else:
                pass

        except:
            pass
############################

mission1=td.Mission('Start to fetching and insert into database (Only be stopped manually)')
for aftererror in range(10): #If error, it can still try.
    try:
        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=["a","e","i","o","u"]) #fetching 'everything' from twitter.

    except Exception as e:
        print(str(e))
        time.sleep(5)
mission1.end()