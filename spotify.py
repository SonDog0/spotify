import requests
import base64
import json
import sys
import pymysql
import logging

logging.info('commit test')

try :
    conn = pymysql.connect(host ='localhost' , user = 'root' , password ='gaion00' , db = 'spotify')
    cursor = conn.cursor()
    pass

except:
    logging.error('can not connect db')
    pass

sql = """ select * from artist_genre """
cursor.execute(sql)
conn.commit()
result =cursor.fetchall()

for i in result:
    print(i)




sys.exit(0)
client_id = "7d4ec8429c3b46eda879a55aa04704bc"
client_secret = "f6e0d19854b74bb48e833c64d7e78764"
endpoint = "https://accounts.spotify.com/api/token"

# python 3.x 버전
encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

headers = {"Authorization": "Basic {}".format(encoded)}

payload = {"grant_type": "client_credentials"}

response = requests.post(endpoint, data=payload, headers=headers)

access_token = json.loads(response.text)['access_token']

headers = {"Authorization": "Bearer {}".format(access_token)}

## Spotify Search API
params = {
    "q": "BTS",
    "type": "artist",
    "limit": "1"
}

r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

# print(r.text)
# print(r.status_code)
# print(r.headers)

raw = json.loads(r.text)
print(raw['artists'].keys())

artist_raw = raw['artists']['items'][0]
print(artist_raw)

followers = artist_raw['followers']['total']
genres = artist_raw['genres']
href = artist_raw['href']

# print("followers: " + str(followers))
# print("genres: " + str(genres))
# print("href: " + str(href))
