import tweepy

consumer_key = "TCAQaSJq0qAjsWyKZdakGssNN",
consumer_secret = "o55EyHnqW5NNA05ds29Nvpmg7VkTkpdY2s76EwA6oUfIv8siea",
access_token = "3722095873-ums9qpIH3g7Y3YJ5kvh3nNMbbslg1gvXrY1Tq8K",
access_token_secret = "yhwDe9kOlU0boWNMlsXYxS6CVfkJvJfLF2Y8NoRn5PlXQ"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Creation of the actual interface, using authentication
api = tweepy.API(auth)

query = "india"
max_tweets = 1
tweets = tweepy.Cursor(api.search, q=query).items(max_tweets)

for tweet in tweets:
    print(tweet.created_at)
