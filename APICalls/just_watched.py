from APICalls.twitter_api import GetTwitter
import tweepy as tw
from dotmap import DotMap
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)


def getJustWatched():
    '''Queries Twitter API and gets tweet for just watched string'''

    content_string = '("just watched" OR "watching" OR "just saw" OR "just finished watching") AND (film OR movie)'
    filter_String = "-filter:retweets -filter:links -filter:replies"
    jw_query= '({} {})'.format(content_string, filter_String)

    # instantiate twitter api
    logging.info("Initializing Twitter API")
    getTwitter = GetTwitter()
    max_tweets = 100
    date_since = "2019-06-01"

    logging.info("Getting Tweets from Twitter API")
    tweets_text, tweet_location, tweet_time = getTwitter.getTweetsbyQuery(jw_query, max_tweets,date_since)
    print(DotMap(getTwitter.api.rate_limit_status()).resources.search)

    movies_just_watched = pd.DataFrame([tweets_text,tweet_location,tweet_time]).transpose()
    movies_just_watched.columns = ['tweet','location','time']
    movies_just_watched.to_csv("..//Database//movies_just_watched.csv")


if __name__ == '__main__':
    getJustWatched()