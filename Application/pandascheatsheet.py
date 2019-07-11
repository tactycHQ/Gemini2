import pandas as pd

x_list = [1,2,3,4,5,6,7,8,9,10]
y_list = [10,20,30,40,50,60,70,80,90,100]

df = pd.DataFrame()
df['x'] = x_list
df['y'] = y_list

#LOC
# print(df.loc[:,'x'])
#
# #FILTER
# df1 = df[df['x'] > 5]['y']
# print(df1)

#SET WITHOUT LOC
# df2 = df[df.x> 5]['y']
# df2['y'] = 100
# print(df2)

#SET WITH LOC AFTER FILTER
df.loc[df.x> 5,y] = 5
# df3['y'] = 100
df.reset_index(drop=True,inplace=True)
print(df)

# #CONCAT
# df4 = pd.concat([df3,df3],axis=1)
# df4.columns=['x','y']
# print(df4)
#
# df5 = pd.concat([df,df4],axis=0).reset_index()
# print(df5)

#ITERATE
# for index, row in df.iterrows():
#     if row.x == 5:
#         df.at[index,'new'] = "amazing,stunning"
# print(df)

