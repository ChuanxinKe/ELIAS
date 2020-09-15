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
submission_select='SELECT * FROM submission_process LEFT JOIN \
    (SELECT DISTINCT id as id2, count(id), max(compound_comment),min(compound_comment),avg(compound_comment) FROM comment_process GROUP by id) as a \
    on submission_process.id=a.id2'
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
df_t = pd.read_sql(submission_select, conn)
print('\nCheck Data Structure:\n')
df_t.loc[:,"time"]=pd.to_datetime(df_t['time'],dayfirst=True)
df_b=df_t[df_t["compound_body"].notnull()]
df_b.info()
mission1.end()

mission2=td.Mission('Split groups')
df_t.loc[:,'polarity'] = [polarity_group(i) for i in df_t['compound_title']]
df_t.loc[:,'score_group'] = [score_group(i) for i in df_t['compound_title']]
df_b.loc[:,'polarity'] = [polarity_group(i) for i in df_b['compound_body']]
df_b.loc[:,'score_group'] = [score_group(i) for i in df_b['compound_body']]

mission3=td.Mission('Draw sentiment scores distribution')

fig_s = plt.figure(figsize=(18,10))
ax_t1 = fig_s.add_subplot(221)
ax_t2 = fig_s.add_subplot(222)
ax_b1 = fig_s.add_subplot(223)
ax_b2 = fig_s.add_subplot(224)

sns.distplot(df_t['compound_title'],ax=ax_t1,bins=20,axlabel='Probability Density and Distribuation of Title Compound Value')
t_counts = pd.DataFrame.from_dict(Counter(df_t['polarity']), orient = 'index').reset_index()
t_counts.columns = ['Polarity', 'Count']
t_counts=t_counts.sort_values(by="Count" , ascending=False).reset_index()
sns.barplot(y="Count", x='Polarity', data=t_counts,ax=ax_t2)
for index, row in t_counts.iterrows():
    ax_t2.text(row.name,row["Count"]+float(15),row["Count"], color='red', ha="center")

sns.distplot(df_b['compound_body'],ax=ax_b1,bins=20,axlabel='Probability Density and Distribuation of Body Compound Value')
b_counts = pd.DataFrame.from_dict(Counter(df_b['polarity']), orient = 'index').reset_index()
b_counts.columns = ['Polarity', 'Count']
b_counts=b_counts.sort_values(by="Count" , ascending=False).reset_index()
sns.barplot(y="Count", x='Polarity', data=b_counts,ax=ax_b2)
for index, row in b_counts.iterrows():
    ax_b2.text(row.name,row["Count"]+float(15),row["Count"], color='red', ha="center")

plt.savefig(output+'submission_distribuation.png')

mission4=td.Mission('Draw sentiment scores time series')
df_t.set_index("time",inplace=True)
time_t=df_t.resample('d')['compound_title'].mean()
time_t=time_t.to_frame()
time_t.reset_index(inplace=True)
time_t.columns=["Date","Title Average Compound"]

df_b.set_index("time",inplace=True)
time_b=df_b.resample('d')['compound_body'].mean()
time_b=time_b.to_frame()
time_b.reset_index(inplace=True)
time_b.columns=["Date","Body Average Compound"]

fig_s2 = plt.figure(figsize=(16,10))
ax_t3 = fig_s2.add_subplot(211)
ax_b3 = fig_s2.add_subplot(212)
sns.lineplot(x="Date", y="Title Average Compound",data=time_t,ax=ax_t3)
sns.lineplot(x="Date", y="Body Average Compound",data=time_b,ax=ax_b3)
plt.savefig(output+'submmision_timeseries.png')


mission5=td.Mission('Get Frequency for Title')
t_result = pd.DataFrame()
t_word_all = [word for wordlist in df_t['title_clean'] for word in literal_eval(wordlist)]
t_top_all = pd.DataFrame(Counter(t_word_all).most_common(60))
t_result.loc[:,'all'] = t_top_all[0]
t_result.loc[:,'frequency'] = t_top_all[1]

t_word_positive = [word for wordlist in df_t[df_t['polarity']=='positive']['title_clean'] for word in literal_eval(wordlist)]
t_word_negative = [word for wordlist in df_t[df_t['polarity']=='negative']['title_clean'] for word in literal_eval(wordlist)]
t_word_neutral = [word for wordlist in df_t[df_t['polarity']=='neutral']['title_clean'] for word in literal_eval(wordlist)]

t_top_positive=pd.DataFrame(Counter(t_word_positive).most_common(60))
t_top_negative=pd.DataFrame(Counter(t_word_negative).most_common(60))
t_top_neutral=pd.DataFrame(Counter(t_word_neutral).most_common(60))

t_result.loc[:,'positive'] = t_top_positive[0]
t_result.loc[:,'frequency_p'] = t_top_positive[1]
t_result.loc[:,'negative'] = t_top_negative[0]
t_result.loc[:,'frequency_neg'] = t_top_negative[1]
t_result.loc[:,'neutral'] = t_top_neutral[0]
t_result.loc[:,'frequency_neu'] = t_top_neutral[1]

t_word_a = [word for wordlist in df_t[df_t['score_group']=='(0.7,1)']['title_clean'] for word in literal_eval(wordlist)]
t_word_b = [word for wordlist in df_t[df_t['score_group']=='(0.45,0.7)']['title_clean'] for word in literal_eval(wordlist)]
t_word_c = [word for wordlist in df_t[df_t['score_group']=='(0.25,0.45)']['title_clean'] for word in literal_eval(wordlist)]
t_word_d = [word for wordlist in df_t[df_t['score_group']=='(0.05,0.25)']['title_clean'] for word in literal_eval(wordlist)]
t_word_e = [word for wordlist in df_t[df_t['score_group']=='neutral']['title_clean'] for word in literal_eval(wordlist)]
t_word_f = [word for wordlist in df_t[df_t['score_group']=='(-0.05,-0.25)']['title_clean'] for word in literal_eval(wordlist)]
t_word_g = [word for wordlist in df_t[df_t['score_group']=='(-0.45,-0.25)']['title_clean'] for word in literal_eval(wordlist)]
t_word_h = [word for wordlist in df_t[df_t['score_group']=='(-0.45,-0.7)']['title_clean'] for word in literal_eval(wordlist)]
t_word_i = [word for wordlist in df_t[df_t['score_group']=='(-1,-0.7)']['title_clean'] for word in literal_eval(wordlist)]

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

t_result.to_excel(output+'title_freqnency.xlsx',index=False)
mission5.end()

mission6=td.Mission('Get Frequency for body')
b_result = pd.DataFrame()
b_word_all = [word for wordlist in df_b['body_clean'] for word in literal_eval(wordlist)]
b_top_all = pd.DataFrame(Counter(b_word_all).most_common(60))
b_result.loc[:,'all'] = b_top_all[0]
b_result.loc[:,'frequency'] = b_top_all[1]

b_word_positive = [word for wordlist in df_b[df_b['polarity']=='positive']['body_clean'] for word in literal_eval(wordlist)]
b_word_negative = [word for wordlist in df_b[df_b['polarity']=='negative']['body_clean'] for word in literal_eval(wordlist)]
b_word_neutral = [word for wordlist in df_b[df_b['polarity']=='neutral']['body_clean'] for word in literal_eval(wordlist)]

b_top_positive=pd.DataFrame(Counter(b_word_positive).most_common(60))
b_top_negative=pd.DataFrame(Counter(b_word_negative).most_common(60))
b_top_neutral=pd.DataFrame(Counter(b_word_neutral).most_common(60))

b_result.loc[:,'positive'] = b_top_positive[0]
b_result.loc[:,'frequency_p'] = b_top_positive[1]
b_result.loc[:,'negative'] = b_top_negative[0]
b_result.loc[:,'frequency_neg'] = b_top_negative[1]
b_result.loc[:,'neutral'] = b_top_neutral[0]
b_result.loc[:,'frequency_neu'] = b_top_neutral[1]

b_word_a = [word for wordlist in df_b[df_b['score_group']=='(0.7,1)']['body_clean'] for word in literal_eval(wordlist)]
b_word_b = [word for wordlist in df_b[df_b['score_group']=='(0.45,0.7)']['body_clean'] for word in literal_eval(wordlist)]
b_word_c = [word for wordlist in df_b[df_b['score_group']=='(0.25,0.45)']['body_clean'] for word in literal_eval(wordlist)]
b_word_d = [word for wordlist in df_b[df_b['score_group']=='(0.05,0.25)']['body_clean'] for word in literal_eval(wordlist)]
b_word_e = [word for wordlist in df_b[df_b['score_group']=='neutral']['body_clean'] for word in literal_eval(wordlist)]
b_word_f = [word for wordlist in df_b[df_b['score_group']=='(-0.05,-0.25)']['body_clean'] for word in literal_eval(wordlist)]
b_word_g = [word for wordlist in df_b[df_b['score_group']=='(-0.45,-0.25)']['body_clean'] for word in literal_eval(wordlist)]
b_word_h = [word for wordlist in df_b[df_b['score_group']=='(-0.45,-0.7)']['body_clean'] for word in literal_eval(wordlist)]
b_word_i = [word for wordlist in df_b[df_b['score_group']=='(-1,-0.7)']['body_clean'] for word in literal_eval(wordlist)]

b_word_I=b_word_a
b_word_II=b_word_b
b_word_III=b_word_f
b_word_IV=b_word_i+b_word_h+b_word_g

b_top_I=pd.DataFrame(Counter(b_word_I).most_common(60))
b_top_II=pd.DataFrame(Counter(b_word_II).most_common(60))
b_top_III=pd.DataFrame(Counter(b_word_III).most_common(60))
b_top_IV=pd.DataFrame(Counter(b_word_IV).most_common(60))

b_result.loc[:,'I'] = b_top_I[0]
b_result.loc[:,'frequency_I'] = b_top_I[1]
b_result.loc[:,'II'] = b_top_II[0]
b_result.loc[:,'frequency_II'] = b_top_II[1]
b_result.loc[:,'III'] = b_top_III[0]
b_result.loc[:,'frequency_III'] = b_top_III[1]
b_result.loc[:,'IV'] = b_top_IV[0]
b_result.loc[:,'frequency_IV'] = b_top_IV[1]

b_result.to_excel(output+'body_freqnency.xlsx',index=False)

mission6.end()