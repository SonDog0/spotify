import sys
import os
import boto3
import requests
import base64
import json
import pymysql
import logging
from datetime import datetime
import pandas as pd
import jsonpath
import pyarrow
import json

with open("config") as f:
   config = json.load(f)


client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"

host = "localhost"
port = 3306
username = "root"
database = "spotify"
password = "gaion00"



def main():


    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    headers = get_headers(client_id , client_secret)

    # RDS - aritst id 가져오고
    cursor.execute("select id from artists LIMIT 10")

    # Top Tracks Spotify  가져오고

    top_track_keys = {

        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_url.spotify"

    }
    top_tracks = []
    for (id, ) in cursor.fetchall():
        print(id)
        URL = 'https://api.spotify.com/v1/artists/{}/top-tracks'.format(id)
        params = {
            'country' : 'US'
        }
        r = requests.get(URL, params=params , headers = headers)
        raw = json.loads(r.text)

        for i in raw['tracks']:
            top_track = {}
            for k,v in top_track_keys.items():
                top_track.update({k : jsonpath.jsonpath(i , v)})
                top_track.update({'artist_id' : id})
                top_tracks.append(top_track)

    # track_ids
    track_ids = [i['id'][0] for i in top_tracks]

    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('top-tracks.parquet' , engine='pyarrow' , compression='snappy')


    dt = datetime.utcnow().strftime("%Y-%m-%d")

    s3 = boto3.resource('s3', aws_access_key_id=config['aws_access_key_id'], aws_secret_access_key= config['aws_secret_access_key'])
    object = s3.Object('son-spotify-artist' , 'dt={}/top-tracks.parquet'.format(dt))
    data = open('top-tracks.parquet' , 'rb')
    object.put(Body=data)

    # S3 import





def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers


if __name__=='__main__':
    main()
