import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image

df = pd.read_csv("..//Database//movie_titles.csv",encoding='utf-8-sig')

idx = df[(df['7day'] < 10)].index
df = df.drop(idx)

idx = df[(df['cpd_avg'] < 0.5)].index
df = df.drop(idx)

cpd_mean = df['cpd_avg'].mean()
cpd_std= df['cpd_avg'].std()
_7day_mean = df['7day'].mean()
_7day_std= df['7day'].std()

df['std_cpd'] = df['cpd_avg'].apply(lambda x: (x-cpd_mean)/cpd_std)
df['std_7day'] = df['7day'].apply(lambda x: (x-_7day_mean)/_7day_std)
df['comp_pulse'] = 0.3*df['std_cpd']+0.7*df['std_7day']

comp_pulse_max = df['comp_pulse'].max()
comp_pulse_min= df['comp_pulse'].min()

theo_max = 0.3*(1-cpd_mean)/cpd_std+0.7*(300-_7day_mean)/_7day_std
theo_min = 0.3*(0.8-cpd_mean)/cpd_std+0.7*(0-_7day_mean)/_7day_std
df['pulse'] = df['comp_pulse'].apply(lambda x: (x-theo_min)/(theo_max-theo_min))

pulse = df[['title','release_date','7day','pulse','simple_adj']].copy()

pulse.to_csv("..//Database//pulse2.csv",encoding='utf-8-sig')
print("File written")
#
# titles = ["Spider-Man: Far from Home","Toy Story 4","Knives Out","Midsommar","Five Feet Apart","Murder Mystery","Shaft"]
#
# for title in titles:
#     title_string=","
#     emotions = pulse[(pulse['title'] == title)]['simple_adj']
#     title_string = title_string.join(emotions)
#
#     wordcloud = WordCloud(background_color='white', height=400, width=800, scale=20, max_words=200).generate(title_string)
#     plt.figure(figsize=(20, 10))
#     plt.imshow(wordcloud, interpolation='bilinear')
#     plt.axis("off")
#     plt.title(title)
#     plt.show()
#     print("Complete for {}".format(title))
