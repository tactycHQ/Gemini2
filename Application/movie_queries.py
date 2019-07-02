#Gets data from TMDB and processes data to generate queries
#Pulls tweets based on queries
from APICalls.movies_title import TMDBRequests
from APICalls.twitter_api import GetTwitter
from tqdm import tqdm
import pandas as pd
import numpy as np
from itertools import islice
from dotmap import DotMap
import logging
logging.basicConfig(level=logging.INFO)

class MovieQueries():

    def __init__(self):
        pass

    def getMovieTitles(self):
        '''Loads TMDB data '''
        self.movie_title = pd.read_csv("..//Database/movie_titles.csv")

    def getUniqueTitles(self):
        '''removes duplicate titles from TMDB data'''
        unique_titles = np.unique(self.movie_title['title'])
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
        combination = '("{}" OR {} OR {} OR {} {} {})'.format(title, self.cleanTitle(title),self.removeSpaces(title),self.quotesTitle(title),content_string, filter_String)
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

    def getTweets(self, batch_size):
        '''Queries Twitter API and gets tweet for each title. Cleans the tweet and returns a dataframe of [title, raw tweets, clean tweets]'''

        # get titles to query from database
        self.getMovieTitles()

        # create twitter queries
        self.createTwitterQueries()

        # instantate twitter api
        logging.info("Initializing Twitter API")
        getTwitter = GetTwitter()
        max_tweets = 100
        date_since = "2019-06-01"

        #start query process
        rawTweets = []
        tweetId = []
        spacyTweets = []
        vaderTweets=[]
        tweetTime = []
        tweetLocation = []
        titleTracker = []
        counter=0
        tweets_counter = 0

        logging.info("Getting Tweets from Twitter API")
        for index, row in tqdm(self.query_df.iterrows()):
            if counter < batch_size:
                try:
                    tweet_id, tweet_text, tweet_location, tweet_time = getTwitter.getTweetsbyQuery(row['queryString'], max_tweets, date_since)
                    spacy_clean = getTwitter.spacy_clean(tweet_text)
                    vader_clean = getTwitter.vader_clean(tweet_text)
                    tweetId.append(tweet_id)
                    rawTweets.append(tweet_text)
                    spacyTweets.append(spacy_clean)
                    vaderTweets.append(vader_clean)
                    tweetTime.append(tweet_time)
                    tweetLocation.append(tweet_location)
                    titleTracker.append(row['title'])
                    counter+=1
                    tweets_counter+=len(tweet_text)
                    print(DotMap(getTwitter.api.rate_limit_status()).resources.search)
                except Exception as ex:
                    print(ex)

        logging.info("Received tweets for {} titles".format(counter))
        logging.info("{} tweets recieved from Twitter API".format(tweets_counter))

        #write query results to dataframe pickle
        queryResults = pd.DataFrame({'tweetId':tweetId,
                                     'title':titleTracker,
                                     'time':tweetTime,
                                     'rawTweets':rawTweets,
                                     'spacyTweets':spacyTweets,
                                     'vaderTweets':vaderTweets,
                                     'location':tweetLocation
                                     })

        queryResults = self.flatten_queries(queryResults)
        queryResults.to_pickle("..//Database//movie_tweets.pkl")
        queryResults.to_csv("..//Database//movie_tweets.csv")
        logging.info("Tweets saved to file")

        return queryResults

    def flatten_queries(self, query_df):
        rawTweets = []
        tweetIds = []
        spacyTweets = []
        vaderTweets=[]
        tweetTimes = []
        tweetLocations = []
        titles = []
        titleTracker = []

        for index, row in query_df.iterrows():
            for id, tw_time, raw, spacytw, vadertw, location in zip(row.tweetId, row.time, row.rawTweets,row.spacyTweets,row.vaderTweets, row.location):
                tweetIds.append(id)
                tweetTimes.append(tw_time)
                rawTweets.append(raw)
                spacyTweets.append(spacytw)
                vaderTweets.append(vadertw)
                tweetLocations.append(location)
                titles.append(row.title)

        flat_queries = pd.DataFrame({'tweetId':tweetIds,'tweetTime':tweetTimes,'title':titles,'rawTweet':rawTweets,'spacyTweet':spacyTweets,'vaderTweet':vaderTweets,'location':tweetLocations})
        return flat_queries

if __name__ == '__main__':
    mq = MovieQueries()
    results = mq.getTweets(batch_size=300)













