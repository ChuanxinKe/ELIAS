#LIBRARIES
import pandas as pd
import warnings
from wordcloud import WordCloud
from matplotlib import pyplot as plt
import TerDec as td # Personal module,needs TerDec.py
warnings.filterwarnings('ignore')
############################

df_t = pd.read_excel("./output/tweets_freqnency.xlsx")
df_b = pd.read_excel("./output/body_freqnency.xlsx")
df_c = pd.read_excel("./output/comment_freqnency.xlsx")
output = r'./output/'
outliers = ["course","get","go","school","student","work"]

def draw_joint_word_cloud(df,text="name"):
    fig, axs = plt.subplots(2, 2, figsize = (20, 12))
    fig.tight_layout(pad = 0)
    positive={}
    negative={}
    neutral={}
    all_word={}

    for index, row in df.iterrows():
        index=index
        if row['all'] not in outliers:
            all_word[row['all']]=row['frequency']
        
        if row['positive'] not in outliers:
            positive[row['positive']]=row['frequency_p']
        
        if row['negative'] not in outliers:
            negative[row['negative']]=row['frequency_neg']
        if row['neutral'] not in outliers:
            neutral[row['neutral']]=row['frequency_neu']         
        
    wordcloud = WordCloud(
        background_color='black',
        max_words=50,
        max_font_size=50, 
        scale=5,
        random_state=1,
        collocations=False,
        normalize_plurals=False
    ).fit_words(all_word)

    positive_cloud = WordCloud(
        background_color='white',
        max_words=50,
        max_font_size=50, 
        scale=5,
        random_state=1,
        collocations=False,
        normalize_plurals=False
    ).fit_words(positive)

    neutral_cloud = WordCloud(
        background_color='white',
        max_words=50,
        max_font_size=50, 
        scale=5,
        random_state=1,
        collocations=False,
        normalize_plurals=False
    ).fit_words(neutral)

    negative_cloud = WordCloud(
        background_color='black',
        max_words=50,
        max_font_size=50, 
        scale=5,
        random_state=1,
        collocations=False,
        normalize_plurals=False
    ).fit_words(negative)

    axs[0, 0].imshow(wordcloud)
    axs[0, 0].set_title('Words from All {}'.format(text), fontsize = 28)
    axs[0, 0].axis('off')

    axs[0, 1].imshow(positive_cloud)
    axs[0, 1].set_title('Words from Positive {}'.format(text), fontsize = 28)
    axs[0, 1].axis('off')

    axs[1, 0].imshow(neutral_cloud)
    axs[1, 0].set_title('Words from Neutral {}'.format(text), fontsize = 28)
    axs[1, 0].axis('off')

    axs[1, 1].imshow(negative_cloud)
    axs[1, 1].set_title('Words from Negative {}'.format(text), fontsize = 28)
    axs[1, 1].axis('off')
  
    plt.savefig(output+'{}_joint_cloud.png'.format(text))

draw_joint_word_cloud(df_t,text="Tweets")
draw_joint_word_cloud(df_b,text="Reddit Bodies")
draw_joint_word_cloud(df_c,text="Reddit Comments")