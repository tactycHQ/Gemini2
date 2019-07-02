import spacy
from tqdm import tqdm
from collections import Counter
import numpy as np
import pandas as pd
from vaderSentiment import vaderSentiment
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image
import logging
import re
import operator
logging.basicConfig(level=logging.INFO)
# emotkeys = [
#         "Intense",
#         "Emotional",
#         "Inspiring",
#         "Disappointing",
#         "Depressing",
#         "Cringe",
#         "Uplifting",
#         "Thrilling",
#         "Memories",
#         "Refreshing",
#         "Intelligent",
#         "Visual",
#         "Despair",
#         "Hilarious",
#         "Comedy",
#         "Family",
#         "Exciting",
#         "High Quality",
#         "Adorable",
#         "Admiration",
#         "Adoration",
#         "Aesthetic Appreciation",
#         "Pretentious",
#         "Boring",
#         "Disgusting",
#         "Apathy",
#         "Awkward",
#         "Confusing",
#         "Enthralling",
#         "Empathetic",
#         "Joy",
#         "Satisfying",
#         "Triumphant"
#     ]
emotkeys = ["Adorable",
            "Breathtaking",
            "Cast",
            "Character",
            "Clever",
            "Confusing",
            "Cringe",
            "Cute",
            "Depressing",
            "Disappointing",
            "Disgusting",
            "Drama",
            "Emotional",
            "Enjoyable",
            "Entertaining",
            "Enthralling",
            "Family",
            "Fights",
            "Fresh",
            "Gorgeous",
            "Heartfelt",
            "Hilarious",
            "Horror",
            "Humorous",
            "Impressive",
            "Inspiring",
            "Intelligent",
            "Intense",
            "Joy",
            "Lacking",
            "Masterpiece",
            "Music",
            "Nightmares",
            "Pretentious",
            "Satisfying",
            "Smart",
            "Speechless",
            "Stunning",
            "Subvert",
            "Superb",
            "Thoughtful",
            "Triumphant",
            "Uplifting",
            "Visuals"]

class Sentiments():

    def __init__(self):
        self.nlp = spacy.load("en_core_web_lg")
        self.movie_tweets = pd.read_pickle("..//Database//movie_tweets.pkl")
        self.emot_by_tweet = None
        print("Spacy is loaded")

    def getVaderscores(self):
        logging.info("----Calculating Sentiment Scores----")
        tweets = self.movie_tweets['vaderTweet']
        vader = vaderSentiment.SentimentIntensityAnalyzer()

        sentScores_neg = []
        sentScores_neu = []
        sentScores_pos = []
        sentScores_cpd = []
        for tweet in tqdm(tweets):
            doc = self.nlp(tweet)
            sentScore = vader.polarity_scores(str(doc))
            sentScores_neg.append(sentScore['neg'])
            sentScores_neu.append(sentScore['neu'])
            sentScores_pos.append(sentScore['pos'])
            sentScores_cpd.append(sentScore['compound'])

        scores_df = pd.DataFrame({'neg':sentScores_neg,
                                  'neu':sentScores_neu,
                                  'pos':sentScores_pos,
                                  'cpd':sentScores_cpd})
        self.movie_tweets = pd.concat([self.movie_tweets,scores_df],axis=1)
        self.movie_tweets.to_pickle("..//Database//movie_tweets.pkl")
        self.movie_tweets.to_csv("..//Database//movie_tweets.csv")
        logging.info("----Scored Appended to Processed Dataframe----")

    def getEmotions(self):
        '''returns dataframe of tweets with emotions tagged in discrete columns'''
        df = self.movie_tweets
        emot_threshold = 0.55
        cpd_threshold = 0.90

        logging.info("----Selecting tweets within threshold parameters----")
        indexNames = df[(df['cpd'] < cpd_threshold)].index
        emot_by_tweet = df.drop(indexNames)
        logging.info("----Selected emotions dataframe has {} tweets----".format(emot_by_tweet.shape[0]))

        logging.info("----Evaluating Emotions----")
        for emot in emotkeys:
            emot_by_tweet[emot] = np.nan

        for index, row in tqdm(emot_by_tweet.iterrows()):
            doc = self.nlp(row.spacyTweet)
            for emot in emotkeys:
                emotVal = float(doc.similarity(self.nlp(emot)))
                if emotVal > emot_threshold:
                    emot_by_tweet.at[index,emot] = emotVal

        emot_by_tweet.reset_index(drop=True,inplace=True)
        self.emot_by_tweet = emot_by_tweet
        self.emot_by_tweet.to_pickle("..//Database//emot_by_tweet.pkl")
        self.emot_by_tweet.to_csv("..//Database//emot_by_tweet.csv")
        logging.info("----Emotions Appended to Processed Dataframe----")

    def wordcloudEmotions(self):

        text_emotions = ' '
        text_emotions = (text_emotions.join(df['TopEmotions'].tolist()))
        text_emotions = re.sub(' +'," ",text_emotions)

        counts=Counter(text_emotions.split())
        print(counts)

        wordcloud = WordCloud(height=400, width=800, scale=20, max_words=200).generate(text_emotions)
        plt.figure(figsize=(20, 10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

        return df

    def wordcloudTitle(self,df):
        title_string = ','
        allTitles = df['title'].values
        title_string = title_string.join(allTitles)

        counts = Counter(allTitles)
        print(counts)

        wordcloud = WordCloud(height=400, width=800, scale=20, max_words=200).generate(title_string)
        plt.figure(figsize=(20, 10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

    def tagEmotionstoTitle(self):
        df = self.emot_by_tweet
        emot_col = df.columns.values[8:] # 8 is the index numbers where emotions start
        emot_df = df.loc[:,emot_col]
        df['TopEmotions'] = emot_df.apply(lambda x: ' '.join(x[x.notnull()].index),axis=1)

        top_emotions_by_title = df[['title','TopEmotions']].copy()
        top_emotions_by_title.to_pickle('..//Database//emotions_by_title.pkl')
        top_emotions_by_title.to_csv('..//Database//emotions_by_title.csv')
        return top_emotions_by_title



if __name__ == '__main__':

    Sentiments = Sentiments()
    Sentiments.getVaderscores()
    Sentiments.getEmotions()
    # emotions_by_tweet = pd.read_pickle("..//Database//emotions_by_tweet.pkl")
    # emotions_title = Sentiments.tagEmotionstoTitle(emotions_by_tweet)
    # Sentiments.wordcloudEmotions(emotions_title)
    # Sentiments.wordcloudTitle(emotions_title)





