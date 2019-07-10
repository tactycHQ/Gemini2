import tweepy as tw
import numpy as np
from dotmap import DotMap
import pandas as pd
import json
import codecs
import unidecode
import re
import spacy
from APICalls.config import get_config_from_json


class GetTwitter():

    def __init__(self):
        self.nlp = spacy.load('en_core_web_lg')
        self.contraction_mapping = {"ain't": "is not", "aren't": "are not", "can't": "cannot",
                               "can't've": "cannot have", "'cause": "because", "could've": "could have",
                               "couldn't": "could not", "couldn't've": "could not have", "didn't": "did not",
                               "doesn't": "does not", "don't": "do not", "hadn't": "had not",
                               "hadn't've": "had not have", "hasn't": "has not", "haven't": "have not",
                               "he'd": "he would", "he'd've": "he would have", "he'll": "he will",
                               "he'll've": "he will have", "he's": "he is", "how'd": "how did",
                               "how'd'y": "how do you", "how'll": "how will", "how's": "how is",
                               "I'd": "I would", "I'd've": "I would have", "I'll": "I will",
                               "I'll've": "I will have", "I'm": "I am", "I've": "I have",
                               "i'd": "i would", "i'd've": "i would have", "i'll": "i will",
                               "i'll've": "i will have", "i'm": "i am", "i've": "i have",
                               "isn't": "is not", "it'd": "it would", "it'd've": "it would have",
                               "it'll": "it will", "it'll've": "it will have", "it's": "it is",
                               "let's": "let us", "ma'am": "madam", "mayn't": "may not",
                               "might've": "might have", "mightn't": "might not", "mightn't've": "might not have",
                               "must've": "must have", "mustn't": "must not", "mustn't've": "must not have",
                               "needn't": "need not", "needn't've": "need not have", "o'clock": "of the clock",
                               "oughtn't": "ought not", "oughtn't've": "ought not have", "shan't": "shall not",
                               "sha'n't": "shall not", "shan't've": "shall not have", "she'd": "she would",
                               "she'd've": "she would have", "she'll": "she will", "she'll've": "she will have",
                               "she's": "she is", "should've": "should have", "shouldn't": "should not",
                               "shouldn't've": "should not have", "so've": "so have", "so's": "so as",
                               "this's": "this is",
                               "that'd": "that would", "that'd've": "that would have", "that's": "that is",
                               "there'd": "there would", "there'd've": "there would have", "there's": "there is",
                               "here's": "here is",
                               "they'd": "they would", "they'd've": "they would have", "they'll": "they will",
                               "they'll've": "they will have", "they're": "they are", "they've": "they have",
                               "to've": "to have", "wasn't": "was not", "we'd": "we would",
                               "we'd've": "we would have", "we'll": "we will", "we'll've": "we will have",
                               "we're": "we are", "we've": "we have", "weren't": "were not",
                               "what'll": "what will", "what'll've": "what will have", "what're": "what are",
                               "what's": "what is", "what've": "what have", "when's": "when is",
                               "when've": "when have", "where'd": "where did", "where's": "where is",
                               "where've": "where have", "who'll": "who will", "who'll've": "who will have",
                               "who's": "who is", "who've": "who have", "why's": "why is",
                               "why've": "why have", "will've": "will have", "won't": "will not",
                               "won't've": "will not have", "would've": "would have", "wouldn't": "would not",
                               "wouldn't've": "would not have", "y'all": "you all", "y'all'd": "you all would",
                               "y'all'd've": "you all would have", "y'all're": "you all are",
                               "y'all've": "you all have",
                               "you'd": "you would", "you'd've": "you would have", "you'll": "you will",
                               "you'll've": "you will have", "you're": "you are", "you've": "you have"}
        self.keys = get_config_from_json('..//Keys//keys.json')
        self.auth = tw.AppAuthHandler(self.keys.twitter_keys.consumer_key, self.keys.twitter_keys.consumer_secret)
        try:
            self.api = tw.API(self.auth, wait_on_rate_limit=True)
            limit = self.api.rate_limit_status()
            limit = DotMap(limit)
            print(limit.resources.search)
        except Exception as ex:
            print(ex)

    def getTweetsbyQuery(self,query,max_tweets,date_since):
        tweet_id = []
        tweet_text = []
        tweet_location = []
        tweet_time= []

        date_since = date_since
        try:
            tweets = tw.Cursor(self.api.search,
                      q=query,
                      count=max_tweets,
                      tweet_mode = 'extended',
                      result_type = 'mixed',
                      lang="en",
                      since=date_since).items(max_tweets)
        except Exception as ex:
            print(ex)
        for tweet in tweets:
            tweet_id.append(tweet.id)
            tweet_text.append(tweet.full_text)
            tweet_location.append(tweet.user.location)
            tweet_time.append(tweet.created_at)

        return tweet_id,tweet_text, tweet_location, tweet_time

    def spacy_clean(self,tweetlist):
        clean_tweets = []
        for tweet in tweetlist:
            clean_tweets.append(self.spacy_cleaner(tweet))
        return clean_tweets

    def vader_clean(self,tweetlist):
        clean_tweets = []
        for tweet in tweetlist:
            clean_tweets.append(self.vader_cleaner(tweet))
        return clean_tweets

    def spacy_cleaner(self,text):
        # try:
        #     decoded = unidecode.unidecode(codecs.decode(text, 'unicode_escape'))
        # except:
        #     decoded = unidecode.unidecode(text)
        apostrophe_handled = re.sub("’", "'", text)
        expanded = ' '.join([self.contraction_mapping[t] if t in self.contraction_mapping else t for t in apostrophe_handled.split(" ")])
        parsed = self.nlp(expanded)
        final_tokens = []
        for t in parsed:
            if t.is_punct or t.is_space or t.like_num or t.like_url or str(t).startswith('@'):
                pass
            else:
                if t.lemma_ == '-PRON-':
                    # final_tokens.append(str(t))
                    pass
                else:
                    sc_removed = re.sub("[^a-zA-Z]", '', str(t.lemma_))
                    if len(sc_removed) > 1:
                        final_tokens.append(sc_removed)
        joined = ' '.join(final_tokens)
        spell_corrected = re.sub(r'(.)\1+', r'\1\1', joined)
        return spell_corrected

    def vader_cleaner(self, text):
        # try:
        #     decoded = unidecode.unidecode(codecs.decode(text, 'unicode_escape'))
        # except:
        #     decoded = unidecode.unidecode(text)
        apostrophe_handled = re.sub("’", "'", text)
        expanded = ' '.join([self.contraction_mapping[t] if t in self.contraction_mapping else t for t in apostrophe_handled.split(" ")])
        parsed = self.nlp(expanded)
        final_tokens = []
        for t in parsed:
            if t.is_punct or t.is_space or t.like_num or t.like_url or str(t).startswith('@'):
                pass
            else:
                sc_removed = re.sub("[^a-zA-Z]", '', str(t))
                if len(sc_removed) > 1:
                    final_tokens.append(sc_removed)
        joined = ' '.join(final_tokens)
        spell_corrected = re.sub(r'(.)\1+', r'\1\1', joined)
        return spell_corrected


if __name__ == '__main__':
    twitter = GetTwitter()
    query = ['Julie of the Wolves']
    print(query[0])
    tweet_id, tweet_text, tweet_location, tweet_time = twitter.getTweetsbyQuery(query[0], max_tweets=300, date_since="2019-06-01")
    print(tweet_id, tweet_text, tweet_location, tweet_time)



























































