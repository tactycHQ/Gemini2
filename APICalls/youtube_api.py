#Get YouTube Data Retrieval Script
import numpy as np
from apiclient.discovery import build
import pandas as pd

DEVELOPER_KEY = "AIzaSyDzJxQraxwmcFdkd1DhHKfzSv_cDxwvjgI"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
youtube_object = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

def youtube_search(query, max_results):
    '''
    Search function to query YouTube based on query word
    :param query: content name to be queried
    :param max_results: max results to be returned
    :return: numpy array of video ids, title, description and thumbnail
    '''
    search_keyword = youtube_object.search().list(q=query, part="id, snippet",maxResults=max_results).execute()
    results = search_keyword.get("items", [])

    video_id = []
    video_title = []
    video_description = []
    video_thumbnail = []

    for result in results:
        if result['id']['kind'] == "youtube#video":
            video_id.append(result["id"]["videoId"])
            video_title.append(result["snippet"]["title"])
            video_description.append(result["snippet"]["description"])
            video_thumbnail.append(result['snippet']['thumbnails']['default']['url'])

    video_id=np.array(video_id)
    video_title = np.array(video_title)
    video_description = np.array(video_description)
    video_thumbnail = np.array(video_thumbnail)
    youtube_data = np.row_stack((video_id,video_title,video_description,video_thumbnail))

    return youtube_data

def getComments(youtube_data):
    '''
    Retrieves comments from video ids
    :param youtube_data: numpy array recd from search function
    :return: numpy array of youtube comments
    '''

    video_ids = youtube_data[0]
    all_comments=[]
    for id in video_ids:
        request = youtube_object.commentThreads().list(part="snippet,replies", videoId=id)
        response = request.execute()
        comments = []
        for result in response["items"]:
            comments.append(result["snippet"]["topLevelComment"]["snippet"]["textDisplay"])
        all_comments.append(comments)

    all_comments=np.array(all_comments).T
    df = pd.DataFrame(all_comments)
    df.to_csv('comments.csv', index=False)

    return all_comments

if __name__ == "__main__":
    youtube_data = youtube_search('avengers', max_results=10)
    youtube_comments = getComments(youtube_data)