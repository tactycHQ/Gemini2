from APICalls.twitter_api import GetTwitter
import tweepy as tw
from dotmap import DotMap
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)


def getJustWatched():
    '''Queries Twitter API and gets tweet for just watched string'''

    content_string = '("just watched" OR "watching" OR "just saw" OR "just finished watching") AND (film OR movie)'
    filter_string = "-filter:retweets -filter:links -filter:replies"
    excl_string = "-spiderman OR -spider-Man OR -toy OR -midsommar"
    jw_query= '({} {} {})'.format(content_string, excl_string,filter_string)

    # instantiate twitter api
    logging.info("Initializing Twitter API")
    getTwitter = GetTwitter()
    max_tweets = 1000
    date_since = "2019-06-01"

    logging.info("Getting Tweets from Twitter API")
    tweet_id,tweet_text, tweet_location, tweet_time = getTwitter.getTweetsbyQuery(jw_query, max_tweets,date_since)
    print(DotMap(getTwitter.api.rate_limit_status()).resources.search)

    movies_just_watched = pd.DataFrame([tweet_id,tweet_text, tweet_location, tweet_time]).transpose()
    movies_just_watched.columns = ['id','tweet','location','time']
    movies_just_watched.to_csv("..//Database//movies_just_watched.csv")


if __name__ == '__main__':
    getJustWatched()