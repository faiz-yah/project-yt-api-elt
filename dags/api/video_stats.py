import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import date

from utils.utils import cal_time_taken

from airflow.decorators import task
from airflow.models import Variable


#load_dotenv(dotenv_path="./.env")

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResult = 5


#@cal_time_taken
@task
def get_playlist_id():
    
    url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}'
    
    try:
        response = requests.get(url)
        
        response.raise_for_status()
        
        data = response.json()
        
        channel_items = data["items"][0]
        
        playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        
        print(f'Completed getting playlist_id of: {playlistId}')
        
        return playlistId
    
    except requests.exceptions.RequestException as e:
        raise e
        
#@cal_time_taken
@task
def get_video_ids(playlistId):
    
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResult}&playlistId={playlistId}&key={API_KEY}"
    
    video_ids = []
    pageToken = None
    count = 1
    
    try:
        while True:
            url = base_url
            
            # If pageToken exist, this part will be True, so we add in the actual pageToken value
            if pageToken:
                url += f"&pageToken={pageToken}"

            # Or else the initial run will use the base url without pageToken 
            response = requests.get(url)
            
            response.raise_for_status()
            
            data = response.json()
            
            print(f"Obtained data for the {count} page")
            
            # Since the videoId output is more than once, we loop through each one and append to the main list
            # for i in range(len(data["items"])):
            #     video_id = data["items"][i]["contentDetails"]["videoId"]
            #     video_ids.append(video_id)

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            
            # Update the pageToken with current's page information to proceed to next page
            pageToken = data.get('nextPageToken')
            
            count += 1
            
            # If at the last page where there is no more pagetoken
            if not pageToken:
                break
            
        return video_ids
        
    except requests.exceptions.RequestException as e:
        raise e

# @cal_time_taken
# def get_videos_stats(video_id_list):
    
#     video_stats_df = []
    
#     for video_id in video_id_list:
#         url = f'https://youtube.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={API_KEY}'
        
#         response = requests.get(url)
        
#         response.raise_for_status()
        
#         data = response.json()
        
#         for item in data.get("items", []):
#             videoId = item["id"]
#             stats = item['statistics']
            
#             viewCount = stats["viewCount"]
#             likeCount = stats["likeCount"]
#             commentCount = stats["commentCount"]

#             video_stats_df.append({
#                 'videoId': videoId,
#                 'viewCount': viewCount,
#                 'likeCount': likeCount,
#                 'commentCount': commentCount
#             })
        
#     video_stats_df = pd.DataFrame(video_stats_df)
        
#     return video_stats_df

@task
def get_video_stats(video_ids):
    
    extracted_data = []
    
    def batch_list(video_id_list, batch_size):
        for idx in range(0, len(video_id_list), batch_size):
            yield video_id_list[idx : idx+batch_size]
    
    for batch in batch_list(video_ids, maxResult):
        
        video_ids_str = ",".join(batch)
        
        url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}'
        
        response = requests.get(url)
        
        response.raise_for_status()
        
        data = response.json()
        
        for item in data.get('items', []):
            video_id = item['id']
            snippet = item['snippet']
            contentDetails = item['contentDetails']
            statistics = item['statistics']
        
            video_data = {
                "video_id" : video_id,
                "title": snippet['title'],
                "publishedAt": snippet['publishedAt'],
                "duration": contentDetails['duration'],
                "viewCount": statistics.get('viewCount', None),
                "likeCount": statistics.get('likeCount', None),
                "commentCount": statistics.get('commentCount', None)
            }
            
            extracted_data.append(video_data)
            
    return extracted_data

@task
def save_to_json(extracted_data):
    file_path = f"./data/YTData_{date.today()}.json"
    
    with open(file_path, 'w', encoding="utf-8" ) as output_file:
        json.dump(extracted_data, output_file, indent=4, ensure_ascii=False)
        
    

if __name__ == '__main__':
    playlistID = get_playlist_id(url_playlist_id)
    video_id_list = get_video_ids(playlistID)
    video_stats_dic = get_video_stats(video_id_list)
    save_to_json(video_stats_dic)
    
    print(f"Total videos: {len(video_id_list)}")
    print(f"List of top 5 videos: {video_id_list[:5]}")
    print(f"Videos stats: {video_stats_dic}")
