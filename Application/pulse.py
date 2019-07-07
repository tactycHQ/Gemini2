import pandas as pd

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
df['std_comp_pulse'] = 0.5*df['std_cpd']+0.5*df['std_7day']
df['pulse'] = df['std_comp_pulse'].apply(lambda x: (x*20+50)/100)

df = df.(['title','release_date','overview','pulse','simple_adj']).copy()

df.to_csv("..//Database//pulse.csv",encoding='utf-8-sig')

