import spacy
from adjectives import *
from tqdm import tqdm
from collections import Counter
import statistics
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

emotkeys_csv = pd.read_csv("..//Database/emotkeys.csv", header=0,index_col=False).values.tolist()
emotkeys = [item for items in emotkeys_csv for item in items]
emot_threshold = 0.575
cpd_threshold = 0.70

class Sentiments():

    def __init__(self):
        self.nlp = spacy.load("en_core_web_lg")
        self.movie_tweets = pd.read_pickle("..//Database//movie_tweets.pkl")
        self.movie_title = pd.read_csv("..//Database/movie_titles.csv", index_col=0)
        self.emot_by_tweet = None
        print("Spacy is loaded")

    def getVaderscores(self):
        '''gets tweets from movie_tweets, calculates vader scores and stores them back in movie_tweets'''
        logging.info("----Calculating VADER Sentiment Scores----")
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
        logging.info("----Scored Appended to Processed Dataframe----")

    def getSimpleAdjEmotions(self):
        '''gets tweets from movie_tweets, runs similarity check against emotKeys and
        returns a new dataframe top_tweets with emotions by tweet'''
        logging.info("----Processing simple adjectives----")
        df = self.movie_tweets

        idx = df[(df['cpd'] < cpd_threshold)].index
        df = df.drop(idx)
        logging.info("----Processing simple adjectives on {} tweets----".format(df.shape[0]))

        for index, row in tqdm(df.iterrows()):
            doc = self.nlp(row.vaderTweet)
            adjs = ""
            for i, token in enumerate(doc):
                if token.pos_ == 'ADJ':
                    adjs = adjs + ", " + str(token)
                    df.at[index,'simple_adj'] =adjs[1:]

        self.movie_tweets = df.reset_index(drop=True)

    def getEmotions(self):
        '''gets tweets from movie_tweets, runs similarity check against emotKeys and
        returns a new dataframe top_tweets with emotions by tweet'''
        logging.info("----Processing emotions using similarity scores----")
        df = self.movie_tweets

        idx = df[(df['cpd'] < cpd_threshold)].index
        emot_by_tweet = df.drop(idx)

        logging.info("----Processing similarity scores on {} tweets----".format(emot_by_tweet.shape[0]))

        for emot in emotkeys:
            emot_by_tweet[emot] = np.nan

        for index, row in tqdm(emot_by_tweet.iterrows()):
            doc = self.nlp(row.spacyTweet)
            for emot in emotkeys:
                emotVal = float(doc.similarity(self.nlp(emot)))
                if emotVal > emot_threshold:
                    emot_by_tweet.at[index,emot] = emotVal

        emot_by_tweet.reset_index(drop=True,inplace=True)

        self.top_tweets = emot_by_tweet

    def tagSimpleAdjtoTitle(self):
        logging.info("---------Tagging simple adjectices movie_title---------")

        # self.top_tweets = pd.read_csv("..//Database//top_tweets.csv",header=0, index_col=0)
        df = self.movie_tweets.dropna(subset=['simple_adj'])
        movies_df = self.movie_title

        for i, r in movies_df.iterrows():
            adj = []
            cpd = []
            for j, row in df.iterrows():
                if row.title == r.title:
                    if row.simple_adj:
                        adj.append(row.simple_adj)
                    if row.cpd:
                        cpd.append(row.cpd)

            simple_adjs = str(adj)

            if cpd:
                cpd_avg = statistics.mean(cpd)
            else:
                cpd_avg = 0

            movies_df.at[i,'simple_adj'] = simple_adjs
            movies_df.at[i, 'cpd_avg'] = cpd_avg

        self.movie_title = movies_df.reset_index(drop=True)

    def tagAdjectives(self):
        '''gets tweets from movie_tweets, gets adjectives and stores them back in movie_tweets'''
        df = self.movie_tweets
        logging.info("----Evaluating Adjectives----")

        for index, row in tqdm(df.iterrows()):
            doc = self.nlp(row.vaderTweet)
            adj = findSVAOs(doc)
            desc = ""
            for a in adj:
                desc = desc + " ".join(a) + ", "
                df.at[index, 'adjectives'] = desc[:-2]

        self.movie_tweets = df
        logging.info("----Adjectives Appended to Movie_Tweets----")

    def tagEmotionstoTitle(self):
        logging.info("---------Tagging emotions from similarity scores to movie_title---------")

        # self.top_tweets = pd.read_csv("..//Database//top_tweets.csv",header=0, index_col=0)
        df = self.top_tweets
        movies_df = self.movie_title

        emot_col = df.columns.values[13:] # 13 is the index numbers where emotions start
        emot_df = df.loc[:,emot_col]
        df['TopEmotions'] = emot_df.apply(lambda x: ','.join(x[x.notnull()].index),axis=1)

        for i, r in movies_df.iterrows():
            emot = []
            for j, row in df.iterrows():
                if row.title == r.title:
                    if row.TopEmotions:
                        emot.append(row.TopEmotions)

            emotions = ','.join(emot)
            movies_df.at[i,'title'] = r.title
            movies_df.at[i,'topEmotions'] = emotions

        self.movie_title = movies_df

    def updateDB(self):
        try:

            self.movie_title.to_csv("..//Database//movie_titles.csv",encoding='utf-8-sig')
            logging.info("movie_titles.csv saved")
            self.movie_tweets.to_pickle("..//Database//movie_tweets.pkl")
            logging.info("movie_tweets.pkl saved")
            self.movie_tweets.to_csv("..//Database//movie_tweets.csv",encoding='utf-8-sig')
            logging.info("movie_tweets.csv saved")
            # self.top_tweets.to_pickle("..//Database//top_tweets.pkl")
            # logging.info("top_tweets.pkl saved")
            # self.top_tweets.to_csv("..//Database//top_tweets.csv",encoding='utf-8')
            # logging.info("top_tweets.csv saved")
            logging.info("All database successfully saved")
        except Exception as ex:
            logging.info("Error saving down one of the final files")
            logging.info(ex)

    # def wordcloudEmotions(self):
    #     "Creates word cloud of top emotions in top titles"
    #
    #     df = self.top_titles
    #     text_emotions = ' '
    #     text_emotions = (text_emotions.join(df['TopEmotions'].tolist()))
    #     text_emotions = re.sub(' +'," ",text_emotions)
    #
    #     wordcloud = WordCloud(height=400, width=800, scale=20, max_words=200).generate(text_emotions)
    #     plt.figure(figsize=(20, 10))
    #     plt.imshow(wordcloud, interpolation='bilinear')
    #     plt.axis("off")
    #     plt.show()
    #
    # def wordcloudTitle(self):
    #     "Creates word cloud of top titles"
    #
    #     df = self.top_titles
    #     title_string = ','
    #     allTitles = df['title'].values
    #     title_string = title_string.join(allTitles)
    #
    #     counts = Counter(allTitles)
    #     print(counts)
    #
    #     wordcloud = WordCloud(height=400, width=800, scale=20, max_words=200).generate(title_string)
    #     plt.figure(figsize=(20, 10))
    #     plt.imshow(wordcloud, interpolation='bilinear')
    #     plt.axis("off")
    #     plt.show()



if __name__ == '__main__':
    Sentiments = Sentiments()
    Sentiments.getVaderscores()
    Sentiments.getSimpleAdjEmotions()
    Sentiments.tagSimpleAdjtoTitle()
    # Sentiments.tagAdjectives()
    # Sentiments.getEmotions()
    # Sentiments.tagEmotionstoTitle()
    Sentiments.updateDB()
    # Sentiments.wordcloudEmotions()
    # Sentiments.wordcloudTitle()





