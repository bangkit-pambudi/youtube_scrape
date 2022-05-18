from googleapiclient.discovery import build
import pandas as pd
import json
import os

DEVELOPER_KEY = "AIzaSyBhom-atxIGQs6ViaQ_od-Lrzl-HWPfKu0"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

def youtube_search(id, max_results=50,order="date", token=None, location=None, location_radius=None):

    search_response = youtube.search().list(
        channelId=id,
        type="video",
        pageToken=token,
        order = order,
        part="id,snippet", # Part signifies the different types of data you want 
        maxResults=max_results,
        location=location,
        locationRadius=location_radius).execute()

#     videos=channelId=channelTitle=categoryId=videoId=viewCount=likeCount=dislikeCount=commentCount=favoriteCount=category=tags  = []
    title = []
    channelId = []
    channelTitle = []
    categoryId = []
    videoId = []
    viewCount = []
    likeCount = []
    commentCount = []
    favoriteCount = []
    PublishDate = []

    with open('yt_data.json', 'w', encoding='utf-8') as f:
        json.dump(search_response, f, ensure_ascii=False, indent=4)
    
    for search_result in search_response.get("items", []):
        #pprint.pprint(search_result)
        if search_result["id"]["kind"] == "youtube#video":

            title.append(search_result['snippet']['title']) 
            videoId.append(search_result['id']['videoId'])
            response = youtube.videos().list(
                part='statistics, snippet',
                id=search_result['id']['videoId']).execute()

            channelId.append(response['items'][0]['snippet']['channelId'])
            PublishDate.append(response['items'][0]['snippet']['publishedAt'])
            channelTitle.append(response['items'][0]['snippet']['channelTitle'])
            categoryId.append(response['items'][0]['snippet']['categoryId'])
            favoriteCount.append(response['items'][0]['statistics']['favoriteCount'])
            viewCount.append(response['items'][0]['statistics']['viewCount'])
            likeCount.append(response['items'][0]['statistics']['likeCount'])
 
        if 'commentCount' in response['items'][0]['statistics'].keys():
            commentCount.append(response['items'][0]['statistics']['commentCount'])
        else:
            commentCount.append(0)
	  
#     pprint.pprint(response)
    youtube_dict = {
		'channelId': channelId,
		'channelTitle': channelTitle,
		'title':title,
		'videoId':videoId,
		'viewCount':viewCount,
		'likeCount':likeCount,
		'commentCount':commentCount,
		'favoriteCount':favoriteCount, 
		'PublishDate':PublishDate,}

    return youtube_dict

def video_comments(video_id, max_results=100):
    # retrieve youtube video results
    video_response=youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id,
		maxResults = max_results
    ).execute()
    #pprint.pprint(video_response)

    id = []
    text = []
    likeCount = []
    publishedAt = []
    totalReplyCount = []
    videoId = []
  
    with open('yt_comment.json', 'w', encoding='utf-8') as f:
        json.dump(video_response, f, ensure_ascii=False, indent=4)

    for item in video_response['items']:
            # Extracting comments
        id.append(item['snippet']['topLevelComment']['id'])
        text.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])

        if item['snippet']['totalReplyCount'] == None:
            totalReplyCount.append(0)
        else:
            totalReplyCount.append(item['snippet']['totalReplyCount'])
        
        publishedAt.append(item['snippet']['topLevelComment']['snippet']['publishedAt'])

        if item['snippet']['topLevelComment']['snippet']['likeCount'] == None:
            likeCount.append(0)
        else:
            likeCount.append(item['snippet']['topLevelComment']['snippet']['likeCount'])

    youtube_dict = {
		'id': id, 
		'text': text, 
		'ReplyCount': totalReplyCount,
		'likeCount': likeCount,
		'publishedAt': publishedAt,
		'videoId' : videoId}
    return youtube_dict

def youtube_scrape(channelId):
    output_channel = youtube_search(channelId)
    dict_channel = {
		'videoId': output_channel['videoId'], 
		'PublishDate': output_channel["PublishDate"], 
		'title': output_channel['title'], 
		'viewCount': output_channel['viewCount'], 
		'likeCount': output_channel["likeCount"], 
		'commentCount': output_channel["commentCount"]} 
    df_channel = pd.DataFrame(dict_channel)

    directory = os.path.join('./data',output_channel['channelTitle'][0])
    if not os.path.exists(directory):
        os.makedirs(directory)

    filename = channelId + '.csv'
    df_channel.to_csv(os.path.join(directory,filename))


    for id_video in output_channel['videoId']:
        output_comment = video_comments(id_video)
        videoId = []
        videoId = [id_video] * len(output_comment['id'])
        dict_comment = {
                'videoId' : videoId,
				'IdUser': output_comment['id'], 
				'text': output_comment['text'],
                'ReplyCount': output_comment['ReplyCount'],
                'likeCount': output_comment['likeCount'],
                'publishedAt': output_comment['publishedAt']
                }
        df_comment = pd.DataFrame(dict_comment)
        filename_comment = id_video + '.csv'

        directory_comment = os.path.join(directory,'comment')
        if not os.path.exists(directory_comment):
            os.makedirs(directory_comment)

        df_comment.to_csv(os.path.join(directory_comment,filename_comment))

if __name__ == "__main__":
    youtube_scrape("UCzI8ArgVBHXN3lSz-dI0yRw")