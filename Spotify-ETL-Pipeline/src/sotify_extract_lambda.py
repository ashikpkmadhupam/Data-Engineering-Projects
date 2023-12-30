import spotipy as sp
from spotipy.oauth2 import SpotifyClientCredentials
import json
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    
    client_credential_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    spotify = sp.Spotify(client_credentials_manager=client_credential_manager)
    
    playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF'
    playlist_id = playlist_link.split('/')[4]
    
    response = spotify.playlist_tracks(playlist_id)
    
    file_name = "Spotify_Raw_Data_"+str(datetime.now())+".json"
    
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Body=json.dumps(response),
        Bucket="spotify-end-to-end-project-ashik",
        Key="raw_data/to_process/"+file_name
        )
    
