#Gets data from TMDB and processes data to generate queries
#Pulls tweets based on queries
from APICalls.tmdb_api import TMDBRequests
from APICalls.twitter_api import GetTwitter
from tqdm import tqdm
import pandas as pd
import numpy as np
from itertools import islice
from dotmap import DotMap
import logging
logging.basicConfig(level=logging.INFO)

class Application():

    def __init__(self):
        pass

    def getTMDBfromDB(self):
        '''Loads TMDB data '''
        self.allTitles = pd.read_csv("..//Database/all_tmdb_movies.csv")

    def getUniqueTitles(self):
        '''removes duplicate titles from TMDB data'''
        allTitleNames = self.allTitles['title']
        unique_titles = np.unique(allTitleNames)
        return unique_titles

    def cleanTitle(self, title):
        '''Cleans title name by removing hifens, colons and periods. So "Avengers: Endgame" becomes "Avengers Endgame'''
        # cleanTitle = title.replace(" ", "").lower()
        cleanTitle = title.replace(",", "")
        cleanTitle = cleanTitle.replace(".", "")
        cleanTitle = cleanTitle.replace("-", "")
        cleanTitle = cleanTitle.replace("'", "")
        cleanTitle = cleanTitle.replace(":","")
        return cleanTitle

    def removeSpaces(self, title):
        '''Cleans title name by removing spaces, hifens, colons and periods. So "Avengers: Endgame" becomes "avengersendgame'''
        spacesRemoved = title.replace(" ", "").lower()
        spacesRemoved = spacesRemoved.replace(",", "")
        spacesRemoved = spacesRemoved.replace(".", "")
        spacesRemoved = spacesRemoved.replace("-", "")
        spacesRemoved = spacesRemoved.replace("'", "")
        spacesRemoved = spacesRemoved.replace(":","")
        return spacesRemoved

    def quotesTitle(self, title):
        '''Adds quotes around titles. So [Avengers: Endgame] becomes "Avengers: Endgame" so twitter can use this exact string'''
        return '"{}"'.format(title)

    def queryCombinations(self, title):
        '''Generates a query string for each title'''
        filter_String = "-filter:retweets -filter:links -filter:replies"
        #note can also add -filter:media if needed but that seems to remove most tweets
        content_string = "AND (release OR film OR movie OR watched)"
        combination = '({} OR {} OR {} OR {} {} {})'.format(title, self.cleanTitle(title),self.removeSpaces(title),self.quotesTitle(title),content_string, filter_String)
        return combination

    def createTwitterQueries(self):
        '''Creates query combinations for all titles and writes it to a file to be processed by twitter via getTweets. Returns a dataframe of [title, query string]'''

        titles = self.getUniqueTitles() #get unique titles

        logging.info("Creating queries")
        query_dict = dict()
        for title in titles:
            query_dict.update({title:self.queryCombinations(title)})

        query_df = pd.DataFrame(list(query_dict.values()), index = query_dict.keys()).reset_index()
        query_df.columns = ['title','queryString']
        query_df.to_csv("..//Database//queries_sent.csv")

        self.query_df = query_df
        logging.info("Queries created")
        return query_df

    def getTweets(self,batch_size):
        '''Queries Twitter API and gets tweet for each title. Cleans the tweet and returns a dataframe of [title, raw tweets, clean tweets]'''

        # get titles to query from database
        self.getTMDBfromDB()

        # create twitter queries
        self.createTwitterQueries()

        # instantate twitter api
        logging.info("Initializing Twitter API")
        getTwitter = GetTwitter()
        max_tweets = 100
        date_since = "2019-06-01"

        #start query process
        rawTweets = []
        spacyTweets = []
        vaderTweets=[]
        titleTracker = []
        counter=0

        logging.info("Getting Tweets from Twitter API")
        for index, row in tqdm(self.query_df.iterrows()):
            if counter < batch_size:
                try:
                    tweets_text, tweet_location, tweet_time = getTwitter.getTweetsbyQuery(row['queryString'], max_tweets, date_since)
                    spacy_clean = getTwitter.spacy_clean(tweets_text)
                    vader_clean = getTwitter.vader_clean(tweets_text)
                    rawTweets.append(tweets_text)
                    spacyTweets.append(spacy_clean)
                    vaderTweets.append(vader_clean)
                    titleTracker.append(row['title'])
                    counter+=1
                    print(DotMap(getTwitter.api.rate_limit_status()).resources.search)
                except Exception as ex:
                    print(ex)

        logging.info("Received tweets for {} titles".format(counter))

        #write query results to dataframe pickle
        queryResults = pd.DataFrame({'title':titleTracker,
                                     'rawTweets':rawTweets,
                                     'spacyTweets':spacyTweets,
                                     'vaderTweets':vaderTweets}).astype('object')

        queryResults = self.flatten_queries(queryResults)
        queryResults.to_pickle("..//Database//query_results.pkl")
        queryResults.to_csv("..//Database//query_results.csv")
        logging.info("Tweets saved to file")

        return queryResults

    def flatten_queries(self, query_df):
        title = []
        rawTweet = []
        spacyTweet = []
        vaderTweet = []

        for index, row in query_df.iterrows():
            for raw, spacytw, vadertw in zip(row.rawTweets,row.spacyTweets,row.vaderTweets):
                rawTweet.append(raw)
                spacyTweet.append(spacytw)
                vaderTweet.append(vadertw)
                title.append(row.title)

        flat_queries = pd.DataFrame({'title':title,'rawTweet':rawTweet,'spacyTweet':spacyTweet,'vaderTweet':vaderTweet})
        return flat_queries

if __name__ == '__main__':
    app = Application()
    results = app.getTweets(batch_size=300)













