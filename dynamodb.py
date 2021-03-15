# import logging
# import boto3
# import os
# import sys
# import requests
# import base64
# import json
# import logging
# import pymysql
#
#
# client_id = "74cbd487458843f1ad3f5fa1e914c02f"
# client_secret = "752e4ed11062473f9da9076c4499d51b"
#
# host = "localhost"
# port = 3306
# username = "root"
# database = "spotify"
# password = "gaion00"
#
#
#
# def main():
#
#
#     try:
#         dynamodb = boto3.resource('dynamodb' , region_name ='ap-northeast-2' , endpoint_url='http://dynamodb.ap-bortheast-2.amazon')
#     except:
#         logging.error('could not connect to dynamodb')
#         sys.exit(1)
#
#     try:
#         conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
#         cursor = conn.cursor()
#     except:
#         logging.error("could not connect to rds")
#         sys.exit(1)
#
#     headers = get_headers(client_id, client_secret)
#
#     table = dynamodb.Table('top_tracks')
#
#     cursor.execute("select id from artists LIMIT1")
#
#     for (artist_id ,) in cursor.fetchall():
#
#         URL = 'https://api.spotify.com/v1/artists/{}/top-tracks'.format(artist_id)
#         params = {
#             'country' : 'US'
#         }
#
#         r= requests.get(URL, params=params , headers=headers)
#
#         raw = json.loads(r.text)
#
#
#         for track in raw['tracks']:
#
#             data = {
#                 'artist_id' : artist_id
#             }
#
#             data.update(track)
#
#             table.put_item(Item=data)
#
#
#
#
#
#
# def get_headers(client_id, client_secret):
#
#     endpoint = "https://accounts.spotify.com/api/token"
#     encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')
#
#     headers = {
#         "Authorization": "Basic {}".format(encoded)
#     }
#
#     payload = {
#         "grant_type": "client_credentials"
#     }
#
#     r = requests.post(endpoint, data=payload, headers=headers)
#
#     access_token = json.loads(r.text)['access_token']
#
#     headers = {
#         "Authorization": "Bearer {}".format(access_token)
#     }
#
#     return headers
#
#
# if __name__ == '__main__':
#     main()


import sys
import os
import boto3
import requests
import base64
import json
import logging
import pymysql


client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"

host = "localhost"
port = 3306
username = "root"
database = "spotify"
password = "gaion00"


def main():


    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)

    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    table = dynamodb.Table('top_tracks')

    cursor.execute('SELECT id FROM artists')

    countries = ['US', 'CA']
    for country in countries:
        for (artist_id, ) in cursor.fetchall():

            URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
            params = {
                'country': 'US'
            }

            r = requests.get(URL, params=params, headers=headers)

            raw = json.loads(r.text)

            for track in raw['tracks']:

                data = {
                    'artist_id': artist_id,
                    'country': country
                }

                data.update(track)

                table.put_item(
                    Item=data
                )





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
