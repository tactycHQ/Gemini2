#Gets data from TMDB and processes data to generate queries
#Pulls tweets based on queries
from APICalls.movies_title import TMDBRequests
from APICalls.twitter_api import GetTwitter
from tqdm import tqdm
import datetime
import pandas as pd
import numpy as np
from itertools import islice
from dotmap import DotMap
import logging

logging.basicConfig(level=logging.INFO)

class MovieQueries():

    def __init__(self,reload=0):

        self.movie_title = pd.read_csv("..//Database/movie_titles.csv", index_col=0)
        self.reload = reload

        if self.reload == 1:
            logging.info("Reloading from memory")
            try:
                self.movie_title = self.movie_title.iloc[:,:15]
            except Exception as ex:
                logging.warning("No movie_titles.csv found")
            try:
                self.movie_tweets = pd.read_pickle("..//Database/movie_tweets.pkl")
            except Exception as ex:
                logging.warning("No movie_tweets.pkl found")
        else:
            pass

    def getUniqueTitles(self):
        '''removes duplicate titles from TMDB data'''
        df = self.movie_title
        unique_titles = np.unique(df['title'])

        idx = df[df.title.isin(unique_titles)].index
        category = df.loc[idx,'tmdbCategory'].values.tolist()

        logging.info("------Found {} unique titles".format(unique_titles.shape[0]))
        return unique_titles, category

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

    def queryCombinations(self, title, category):
        '''Generates a query string for each title'''
        filter_String = "-filter:retweets -filter:links -filter:replies"
        #note can also add -filter:media if needed but that seems to remove most tweets
        content_string = "AND (release OR film OR movie OR watched OR teaser OR trailer)"
        combination = '("{}" OR {} OR {} OR {} {} {})'.format(title, self.cleanTitle(title),self.removeSpaces(title),self.quotesTitle(title),content_string, filter_String)
        return combination

    def createTwitterQueries(self):
        '''Creates query combinations for all titles and writes it to a file to be processed by twitter via getTweets. Returns a dataframe of [title, query string]'''

        titles, categories = self.getUniqueTitles() #get unique titles

        logging.info("Creating queries")
        query_dict = dict()
        for title, category in zip(titles, categories):
            query_dict.update({title:self.queryCombinations(title, category)})


        query_df = pd.DataFrame(list(query_dict.values()), index = query_dict.keys()).reset_index()
        query_df.columns = ['title','queryString']
        query_df.to_csv("..//Database//queries_sent.csv")

        self.query_df = query_df
        logging.info("Queries created")
        return query_df

    def getTweets(self, batch_size):
        '''Queries Twitter API and gets tweet for each title. Cleans the tweet and returns a dataframe of [title, raw tweets, clean tweets]'''
        # create twitter queries
        if self.reload ==0:

            self.createTwitterQueries()

            # instantate twitter api
            logging.info("Initializing Twitter API")
            getTwitter = GetTwitter()
            max_tweets = 200
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
            movie_tweets = pd.DataFrame({'tweetId':tweetId,
                                         'title':titleTracker,
                                         'time':tweetTime,
                                         'rawTweets':rawTweets,
                                         'spacyTweets':spacyTweets,
                                         'vaderTweets':vaderTweets,
                                         'location':tweetLocation
                                         })

            self.movie_tweets = self.flatten_queries(movie_tweets).reset_index(drop=True)

        #create query counts and write to database
        self.addQueryCounts()
        self.getEarliestTimeStamp()

        return self.movie_tweets

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

    def addQueryCounts(self):

        logging.info("Add query counts to movie_titles")
        tweets_df = self.movie_tweets
        movies_df = self.movie_title

        currTime = datetime.datetime.utcnow()

        # Collecting counts for past 7 day of query results
        deadline1 = currTime + datetime.timedelta(-1)
        indexNames1 = tweets_df[tweets_df['tweetTime'] < deadline1].index
        tweets_df1 = tweets_df.drop(indexNames1, inplace=False)
        latest_df1 = tweets_df1.reset_index(drop=True)
        counts_df1 = latest_df1['title'].value_counts().rename_axis('title').reset_index(name='1day')

        deadline2 = currTime + datetime.timedelta(-2)
        indexNames2 = tweets_df[tweets_df['tweetTime'] < deadline2].index
        tweets_df2 = tweets_df.drop(indexNames2, inplace=False)
        latest_df2 = tweets_df2.reset_index(drop=True)
        counts_df2 = latest_df2['title'].value_counts().rename_axis('title').reset_index(name='2day')

        deadline3 = currTime + datetime.timedelta(-3)
        indexNames3 = tweets_df[tweets_df['tweetTime'] < deadline3].index
        tweets_df3 = tweets_df.drop(indexNames3, inplace=False)
        latest_df3 = tweets_df3.reset_index(drop=True)
        counts_df3 = latest_df3['title'].value_counts().rename_axis('title').reset_index(name='3day')

        deadline4 = currTime + datetime.timedelta(-4)
        indexNames4 = tweets_df[tweets_df['tweetTime'] < deadline4].index
        tweets_df4 = tweets_df.drop(indexNames4, inplace=False)
        latest_df4 = tweets_df4.reset_index(drop=True)
        counts_df4 = latest_df4['title'].value_counts().rename_axis('title').reset_index(name='4day')

        deadline5 = currTime + datetime.timedelta(-5)
        indexNames5 = tweets_df[tweets_df['tweetTime'] < deadline5].index
        tweets_df5 = tweets_df.drop(indexNames5, inplace=False)
        latest_df5 = tweets_df5.reset_index(drop=True)
        counts_df5 = latest_df5['title'].value_counts().rename_axis('title').reset_index(name='5day')

        deadline6 = currTime + datetime.timedelta(-6)
        indexNames6 = tweets_df[tweets_df['tweetTime'] < deadline6].index
        tweets_df6 = tweets_df.drop(indexNames6, inplace=False)
        latest_df6 = tweets_df6.reset_index(drop=True)
        counts_df6 = latest_df6['title'].value_counts().rename_axis('title').reset_index(name='6day')

        deadline7 = currTime + datetime.timedelta(-7)
        indexNames7 = tweets_df[tweets_df['tweetTime'] < deadline7].index
        tweets_df7 = tweets_df.drop(indexNames7, inplace=False)
        latest_df7 = tweets_df7.reset_index(drop=True)
        counts_df7 = latest_df7['title'].value_counts().rename_axis('title').reset_index(name='7day')

        # Adding counts to movie_titles table
        count_dfs = [counts_df1, counts_df2, counts_df3, counts_df4, counts_df5, counts_df6, counts_df7]
        merged_df = movies_df
        for df in count_dfs:
            merged_df = merged_df.merge(df,how='left').reset_index(drop=True)

        #Calculating delta in queries each day
        merged_df['1day_delta'] = merged_df['1day']
        merged_df['2day_delta'] = merged_df['2day'] - merged_df['1day']
        merged_df['3day_delta'] = merged_df['3day'] - merged_df['2day']
        merged_df['4day_delta'] = merged_df['4day'] - merged_df['3day']
        merged_df['5day_delta'] = merged_df['5day'] - merged_df['4day']
        merged_df['6day_delta'] = merged_df['6day'] - merged_df['5day']
        merged_df['7day_delta'] = merged_df['7day'] - merged_df['6day']

        # Saving updated movie_titles
        self.movie_title = merged_df.reset_index(drop=True)

    def getEarliestTimeStamp(self):
        logging.info("Add earliest tweet time stamp")
        tweets_df = self.movie_tweets
        movies_df = self.movie_title

        for index, row in movies_df.iterrows():
            try:
                earliest_tweet = min(tweets_df.loc[tweets_df['title'] == row.title]['tweetTime'])
            except:
                earliest_tweet = None
            movies_df.at[index,'earliestTweet'] = earliest_tweet
        self.movie_title = movies_df

    def updateDB(self):

        self.movie_title.to_csv("..//Database//movie_titles.csv",encoding='utf-8-sig')
        self.movie_tweets.to_pickle("..//Database//movie_tweets.pkl")
        self.movie_tweets.to_csv("..//Database//movie_tweets.csv",encoding='utf-8-sig')
        logging.info("movie_title and movie_tweets updated")


if __name__ == '__main__':
    mq = MovieQueries(reload=0)
    results = mq.getTweets(batch_size=300)
    mq.updateDB()











