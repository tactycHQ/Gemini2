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
        print("Spacy is loaded")

    def getVaderscores(self, query_results):
        logging.info("----Calculating Sentiment Scores----")
        tweets = query_results['vaderTweet']
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
        query_results_processsed = pd.concat([query_results,scores_df],axis=1)
        query_results_processsed.to_pickle("..//Database//query_results_scores.pkl")
        query_results_processsed.to_csv("..//Database//query_results_scores.csv")
        logging.info("----Scored Appended to Processed Dataframe----")

        return query_results_processsed

    def getEmotions(self, query_results):
        emot_threshold = 0.55
        cpd_threshold = 0.95

        logging.info("----Selecting tweets within threshold parameters----")
        indexNames = query_results[(query_results['cpd'] < cpd_threshold)].index
        emot_by_tweet = query_results.drop(indexNames)
        logging.info("----Selected emotions dataframe has {} tweets----".format(emot_by_tweet.shape[0]))

        logging.info("----Evaluating Emotions----")

        for emot in emotkeys:
            emot_by_tweet[emot] = np.nan

        for index, row in tqdm(emot_by_tweet.iterrows()):
            doc = self.nlp(row.spacyTweet)
            for emot in emotkeys:
                emotVal = doc.similarity(self.nlp(emot))
                if emotVal > emot_threshold:
                    emot_by_tweet.at[index,emot] = emotVal

        emot_by_tweet.reset_index(drop=True,inplace=True)
        emot_by_tweet.to_pickle("..//Database//emotions_by_tweet.pkl")
        emot_by_tweet.to_csv("..//Database//emotions_by_tweet.csv")
        logging.info("----Emotions Appended to Processed Dataframe----")
        return emot_by_tweet

    def wordcloudEmotions(self,emotions_by_tweet):
        text_emotions = ''
        allEmotions = query_emotions['emotions'].values

        for title_em in allEmotions:
            emotions = title_em[0]
            for emotion in emotions:
                text_emotions= text_emotions+str(emotion)+" "

        counts=Counter(text_emotions.split())
        print(counts)

        wordcloud = WordCloud(height=400, width=800, scale=20, max_words=200).generate(text_emotions)
        plt.figure(figsize=(20, 10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

    def wordcloudTitle(self,query_emotions):
        text_title = ''
        allTitles = query_emotions['title'].values
        for title in allTitles: text_title = text_title + str(title) + " "

        counts = Counter(text_title.split())
        print(counts)

        wordcloud = WordCloud(height=400, width=800, scale=20, max_words=200).generate(text_title)
        plt.figure(figsize=(20, 10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

    def tagEmotionstoTitle(self,query_emotions):

        unique_titles = np.unique(query_emotions['title'].values)

        title_emotions = {}
        for title in unique_titles:
            for index, row in query_emotions.iterrows():
                if title == row.title.values:
                    # print(title)
                    title_emotions.setdefault(title,[]).append(row.emotions.values)



if __name__ == '__main__':
    query_results = pd.read_pickle("..//Database//query_results.pkl")
    Sentiments = Sentiments()
    # query_results_processed = Sentiments.getVaderscores(query_results)
    query_results_processed = pd.read_pickle("..//Database//query_results_scores.pkl")
    all_emotions = Sentiments.getEmotions(query_results_processed)
    # all_emotions = pd.read_pickle("..//Database//query_results_emotions.pkl")
    # Sentiments.wordcloudEmotions(all_emotions)
    # Sentiments.wordcloudTitle(all_emotions)
    # Sentiments.tagEmotionstoTitle(all_emotions)




