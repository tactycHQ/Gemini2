import spacy
import pandas as pd
from collections import Counter
from vaderSentiment import vaderSentiment
nlp = spacy.load("en_core_web_lg")
print("Spacy loaded")

class Sentiments():

    emotkeys = [
        "Intense",
        "Emotional",
        "Inspiring",
        "Dissapointing",
        "Uplifting",
        "Cringe",
        "Thrilling",
        "Memories",
        "Refreshing",
        "Intelligent",
        "Visual",
        "Despair",
        "Hilarious",
        "Comedy",
        "Family",
        "Exciting",
        "High Quality",
        "Adorable",
        "Depressing",
        "Admiration",
        "Adoration",
        "Aesthetic Appreciation",
        "Disgusting",
        "Apathy",
        "Awkard",
        "Confusing",
        "Enthralling",
        "Empathetic",
        "Joy",
        "Satisfying",
        "Triumphant"
    ]

    def __init__(self):
        self.nlp = spacy.load("en_core_web_lg")
        print("Spacy is loaded")

    def getVaderscores(self, query_results):
        tweets = query_results['vaderTweets']
        vader = vaderSentiment.SentimentIntensityAnalyzer()

        for tweet in tweets:
            doc = self.nlp(tweet)
            sents = list(doc.sents)
            for s in sents:
                vader.polarity_scores(str(sents))




    # def getSpacyscores(self, tweets):
    #     entertainment = "I still have to watch Bumblebee Itas reboot of the Transformers franchise which be huge part of my childhood and the design of the classic Transformers in the film look too good to have miss I donat even care if itas bad I have to see it at some point"
    #     doc1 = nlp("%s" % entertainment)
    #
    #
    #     nlp_doc2 = []
    #     for sentiment in doc2:
    #         nlp_doc2.append(nlp(sentiment))
    #
    #     sentiment_value = {}
    #     for sentiment in nlp_doc2:
    #         sentiment_value.update({sentiment: doc1.similarity(sentiment)})
    #
    #     topSentiments = Counter(sentiment_value)
    #     high = topSentiments.most_common(5)
    #
    #     for i in high:
    #         print(('{} : {}').format(i[0], i[1]))

    def getEmotions(self, tweets):
        pass

if __name__ == '__main__'
    query_results = pd.read_pickle("..//Database//query_results.pkl")

    Sentiments = Sentiments()






