#LIBRARIES
import pandas as pd
import sqlite3
import warnings
from collections import Counter
from ast import literal_eval
from matplotlib import pyplot as plt
from matplotlib import ticker
import seaborn as sns
from wordcloud import WordCloud
import TerDec as td # Personal module,needs TerDec.py
warnings.filterwarnings('ignore')
############################

mission0=td.Mission('Initial')
conn = sqlite3.connect('cleaned_text.db')
c = conn.cursor()
tweet_select=r'select * from tweets_process'
comment_select=r'select * from comment_process'
output=r'./output/'

def polarity_group(i):
    if i>= 0.05:
        return 'positive'
    elif i<=-0.05:
        return 'negative'
    else:
        return 'neutral'

def score_group(i):
    if i>= 0.7:
        return '(0.7,1)'
    elif i>=0.45:
        return '(0.45,0.7)'
    elif i>=0.25:
        return '(0.25,0.45)'
    elif i>=0.05:
        return '(0.05,0.25)'
    elif i>-0.05:
        return 'neutral'
    elif i>=-0.20:
        return '(-0.05,-0.25)'
    elif i>=-0.45:
        return '(-0.45,-0.25)'
    elif i>=-0.7:
        return '(-0.45,-0.7)'
    else:
        return '(-1,-0.7)'

mission0.end()

mission1=td.Mission('Select data')
df_t = pd.read_sql(tweet_select, conn)
df_c = pd.read_sql(comment_select, conn)
print('\nCheck Data Structure:\n')
df_t.info()
df_c.info()
mission1.end()

mission2=td.Mission('Part for tweets')
df_t.loc[:,'time'] = pd.to_datetime(df_t['timestamp_ms'],unit='ms')
df_t.loc[:,'polarity'] = [polarity_group(i) for i in df_t['compound_tweet']]
df_t.loc[:,'score_group'] = [score_group(i) for i in df_t['compound_tweet']]

mission2_1=td.Mission('Draw sentiment scores distribution')
fig_t = plt.figure(figsize=(18,5))
ax_t1 = fig_t.add_subplot(121)
ax_t2 = fig_t.add_subplot(122)
sns.distplot(df_t['compound_tweet'],ax=ax_t1,bins=20,axlabel='Probability Density and Distribuation of Tweets Compound Value')
t_counts = pd.DataFrame.from_dict(Counter(df_t['polarity']), orient = 'index').reset_index()
t_counts.columns = ['Polarity', 'Count']
t_counts=t_counts.sort_values(by="Count" , ascending=False).reset_index()
sns.barplot(y="Count", x='Polarity', data=t_counts,ax=ax_t2)
for index, row in t_counts.iterrows():
    ax_t2.text(row.name,row["Count"]+float(15),row["Count"], color='red', ha="center")
plt.savefig(output+'tweet_distribuation.png')

mission2_2=td.Mission('Draw sentiment scores time series')
df_t.set_index("time",inplace=True)
time_t=df_t.resample('d')['compound_tweet'].mean()
time_t=time_t.to_frame()
time_t.reset_index(inplace=True)
time_t.columns=["Date","Average Compound"]
fig_t2 = plt.figure(figsize=(16,5))
ax_t3 = fig_t2.add_subplot(111)
sns.lineplot(x="Date", y="Average Compound",data=time_t,ax=ax_t3)
plt.savefig(output+'tweet_timeseries.png')

mission2_3=td.Mission('Get Frequency for All Words')
t_result = pd.DataFrame()
t_word_all = [word for wordlist in df_t['tweet_clean'] for word in literal_eval(wordlist)]
t_top_all = pd.DataFrame(Counter(t_word_all).most_common(60))
t_result.loc[:,'all'] = t_top_all[0]
t_result.loc[:,'frequency'] = t_top_all[1]

mission2_4=td.Mission('Get Frequency for 3 polarities')
t_word_positive = [word for wordlist in df_t[df_t['polarity']=='positive']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_negative = [word for wordlist in df_t[df_t['polarity']=='negative']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_neutral = [word for wordlist in df_t[df_t['polarity']=='neutral']['tweet_clean'] for word in literal_eval(wordlist)]

t_top_positive=pd.DataFrame(Counter(t_word_positive).most_common(60))
t_top_negative=pd.DataFrame(Counter(t_word_negative).most_common(60))
t_top_neutral=pd.DataFrame(Counter(t_word_neutral).most_common(60))

t_result.loc[:,'positive'] = t_top_positive[0]
t_result.loc[:,'frequency_p'] = t_top_positive[1]
t_result.loc[:,'negative'] = t_top_negative[0]
t_result.loc[:,'frequency_neg'] = t_top_negative[1]
t_result.loc[:,'neutral'] = t_top_neutral[0]
t_result.loc[:,'frequency_neu'] = t_top_neutral[1]

mission2_5=td.Mission('Get Frequency for 8 thredholds')
t_word_a = [word for wordlist in df_t[df_t['score_group']=='(0.7,1)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_b = [word for wordlist in df_t[df_t['score_group']=='(0.45,0.7)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_c = [word for wordlist in df_t[df_t['score_group']=='(0.25,0.45)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_d = [word for wordlist in df_t[df_t['score_group']=='(0.05,0.25)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_e = [word for wordlist in df_t[df_t['score_group']=='neutral']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_f = [word for wordlist in df_t[df_t['score_group']=='(-0.05,-0.25)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_g = [word for wordlist in df_t[df_t['score_group']=='(-0.45,-0.25)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_h = [word for wordlist in df_t[df_t['score_group']=='(-0.45,-0.7)']['tweet_clean'] for word in literal_eval(wordlist)]
t_word_i = [word for wordlist in df_t[df_t['score_group']=='(-1,-0.7)']['tweet_clean'] for word in literal_eval(wordlist)]

t_word_I=t_word_a
t_word_II=t_word_b
t_word_III=t_word_f
t_word_IV=t_word_i+t_word_h+t_word_g

t_top_I=pd.DataFrame(Counter(t_word_I).most_common(60))
t_top_II=pd.DataFrame(Counter(t_word_II).most_common(60))
t_top_III=pd.DataFrame(Counter(t_word_III).most_common(60))
t_top_IV=pd.DataFrame(Counter(t_word_IV).most_common(60))

t_result.loc[:,'I'] = t_top_I[0]
t_result.loc[:,'frequency_I'] = t_top_I[1]
t_result.loc[:,'II'] = t_top_II[0]
t_result.loc[:,'frequency_II'] = t_top_II[1]
t_result.loc[:,'III'] = t_top_III[0]
t_result.loc[:,'frequency_III'] = t_top_III[1]
t_result.loc[:,'IV'] = t_top_IV[0]
t_result.loc[:,'frequency_IV'] = t_top_IV[1]

t_result.to_excel(output+'tweets_freqnency.xlsx',index=False)
mission2.end()

mission3=td.Mission('Part for comment')
df_c.loc[:,'polarity'] = [polarity_group(i) for i in df_c['compound_comment']]
df_c.loc[:,'score_group'] = [score_group(i) for i in df_c['compound_comment']]

mission3_1=td.Mission('Draw sentiment scores distribution')
fig_c = plt.figure(figsize=(18,5))
ax_c1 = fig_c.add_subplot(121)
ax_c2 = fig_c.add_subplot(122)
sns.distplot(df_c['compound_comment'],ax=ax_c1,bins=20,axlabel='Probability Density and Distribuation of Comments Compound Value')
c_counts = pd.DataFrame.from_dict(Counter(df_c['polarity']), orient = 'index').reset_index()
c_counts.columns = ['Polarity', 'Count']
c_counts=c_counts.sort_values(by="Count" , ascending=False).reset_index()
sns.barplot(y="Count", x='Polarity', data=c_counts,ax=ax_c2)
for index, row in c_counts.iterrows():
    ax_c2.text(row.name,row["Count"]+float(15),row["Count"], color='red', ha="center")
plt.savefig(output+'comment_distribuation.png')

mission3_2=td.Mission('Get Frequency for All Words')
c_result = pd.DataFrame()
c_word_all = [word for wordlist in df_c['comment_clean'] for word in literal_eval(wordlist)]
c_top_all = pd.DataFrame(Counter(c_word_all).most_common(60))
c_result.loc[:,'all'] = c_top_all[0]
c_result.loc[:,'frequency'] = c_top_all[1]

mission3_3=td.Mission('Get Frequency for 3 polarities')
c_word_positive = [word for wordlist in df_c[df_c['polarity']=='positive']['comment_clean'] for word in literal_eval(wordlist)]
c_word_negative = [word for wordlist in df_c[df_c['polarity']=='negative']['comment_clean'] for word in literal_eval(wordlist)]
c_word_neutral = [word for wordlist in df_c[df_c['polarity']=='neutral']['comment_clean'] for word in literal_eval(wordlist)]

c_top_positive=pd.DataFrame(Counter(c_word_positive).most_common(60))
c_top_negative=pd.DataFrame(Counter(c_word_negative).most_common(60))
c_top_neutral=pd.DataFrame(Counter(c_word_neutral).most_common(60))

c_result.loc[:,'positive'] = c_top_positive[0]
c_result.loc[:,'frequency_p'] = c_top_positive[1]
c_result.loc[:,'negative'] = c_top_negative[0]
c_result.loc[:,'frequency_neg'] = c_top_negative[1]
c_result.loc[:,'neutral'] = c_top_neutral[0]
c_result.loc[:,'frequency_neu'] = c_top_neutral[1]

mission3_4=td.Mission('Get Frequency for 8 thredholds')
c_word_a = [word for wordlist in df_c[df_c['score_group']=='(0.7,1)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_b = [word for wordlist in df_c[df_c['score_group']=='(0.45,0.7)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_c = [word for wordlist in df_c[df_c['score_group']=='(0.25,0.45)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_d = [word for wordlist in df_c[df_c['score_group']=='(0.05,0.25)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_e = [word for wordlist in df_c[df_c['score_group']=='neutral']['comment_clean'] for word in literal_eval(wordlist)]
c_word_f = [word for wordlist in df_c[df_c['score_group']=='(-0.05,-0.25)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_g = [word for wordlist in df_c[df_c['score_group']=='(-0.45,-0.25)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_h = [word for wordlist in df_c[df_c['score_group']=='(-0.45,-0.7)']['comment_clean'] for word in literal_eval(wordlist)]
c_word_i = [word for wordlist in df_c[df_c['score_group']=='(-1,-0.7)']['comment_clean'] for word in literal_eval(wordlist)]

c_word_I=c_word_a
c_word_II=c_word_b
c_word_III=c_word_f
c_word_IV=c_word_i+c_word_h+c_word_g

c_top_I=pd.DataFrame(Counter(c_word_I).most_common(60))
c_top_II=pd.DataFrame(Counter(c_word_II).most_common(60))
c_top_III=pd.DataFrame(Counter(c_word_III).most_common(60))
c_top_IV=pd.DataFrame(Counter(c_word_IV).most_common(60))

c_result.loc[:,'I'] = c_top_I[0]
c_result.loc[:,'frequency_I'] = c_top_I[1]
c_result.loc[:,'II'] = c_top_II[0]
c_result.loc[:,'frequency_II'] = c_top_II[1]
c_result.loc[:,'III'] = c_top_III[0]
c_result.loc[:,'frequency_III'] = c_top_III[1]
c_result.loc[:,'IV'] = c_top_IV[0]
c_result.loc[:,'frequency_IV'] = c_top_IV[1]

c_result.to_excel(output+'comment_freqnency.xlsx',index=False)

mission3.end()