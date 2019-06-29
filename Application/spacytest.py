from wordcloud import WordCloud
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from os import path
from PIL import Image

query_results_processed = pd.read_pickle("..//Database//query_results_scores.pkl")
tweets = query_results_processed['vaderTweet']
text = " ".join(tweet for tweet in tweets)
# print(text)

wordcloud = WordCloud(height=400, width=800, scale= 20,max_words=200).generate(text)

plt.figure(figsize=(20,10))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()