import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime


def fill_date(row) :
    if len(row['release_date']) == 4:
        return row['release_date']+'-01-01'
    else :
        return row['release_date']

def create_album_df(response):
    album_list = []
    for item in response['items'] :
        album= {}
        album['id'] = item['track']['album']['id']
        album['name'] = item['track']['album']['name']
        album['url'] = item['track']['album']['external_urls']['spotify']
        album['release_date'] = item['track']['album']['release_date']
        album['total_tracks'] = item['track']['album']['total_tracks']
        album_list.append(album)
        
    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['id'])
    album_df['release_date'] = album_df.apply(fill_date, axis=1)
    album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    return album_df
    
def create_artist_df(response) :
    artist_list = []
    for item in response['items'] :
        for artist in item['track']['album']['artists']:
            artist_dict = {'id':artist['id'],'name':artist['name'],'url':artist['external_urls']['spotify']}
            artist_list.append(artist_dict)
    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates(subset=['id'])
    return artist_df;
    
    
def create_song_df(response) :
    song_list = []
    for item in response['items'] :
        song = {'id':item['track']['id'], 'name':item['track']['name'],'popularity':item['track']['popularity'],'url':item['track']['external_urls']['spotify'],'duration':item['track']['duration_ms'],'song_added':item['added_at'],'album_id':item['track']['album']['id'],'artist_id':item['track']['album']['artists'][0]['id']}
        song_list.append(song)
    song_df = pd.DataFrame.from_dict(song_list)
    song_df = song_df.drop_duplicates(subset=['id'])
    song_df['song_added'] = pd.to_datetime(song_df['song_added'])
    return song_df
    

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    object_list = s3.list_objects(Bucket="spotify-end-to-end-project-ashik", Prefix="raw_data/to_process")
    
    key_list = []
    json_list = []
    for obj in object_list['Contents'] :
        key_list.append(obj["Key"])
        data = s3.get_object(Bucket="spotify-end-to-end-project-ashik",Key=obj["Key"])
        json_data = json.loads(data['Body'].read())
        json_list.append(json_data)
    
    for data in json_list:
        album_df = create_album_df(data)
        artist_df = create_artist_df(data)
        song_df = create_song_df(data)
        
        album_buf = StringIO()
        album_df.to_csv(album_buf, header=True, index=False)
        album_buf.seek(0)
        album_file_name = "transformed_data/album_data/spotify_album_processed_"+str(datetime.now())+".csv"
        s3.put_object(Bucket="spotify-end-to-end-project-ashik", Body=album_buf.getvalue(), Key=album_file_name)
        
        artist_buf = StringIO()
        artist_df.to_csv(artist_buf, header=True, index=False)
        artist_buf.seek(0)
        artist_file_name = "transformed_data/artist_data/spotify_artist_processed_"+str(datetime.now())+".csv"
        s3.put_object(Bucket="spotify-end-to-end-project-ashik", Body=artist_buf.getvalue(), Key=artist_file_name)
        
        song_buf = StringIO()
        song_df.to_csv(song_buf, header=True, index=False)
        song_buf.seek(0)
        song_file_name = "transformed_data/song_data/spotify_song_processed_"+str(datetime.now())+".csv"
        s3.put_object(Bucket="spotify-end-to-end-project-ashik", Body=song_buf.getvalue(), Key=song_file_name)
    
    for s3_file in key_list:
        
        copy_source = {
            'Bucket': 'spotify-end-to-end-project-ashik',
            'Key': s3_file
        }
        target_file_name = "raw_data/processed/"+s3_file.split("/")[-1]
        s3.copy(copy_source, 'spotify-end-to-end-project-ashik', target_file_name)
        s3.delete_object(Bucket="spotify-end-to-end-project-ashik",Key=s3_file)