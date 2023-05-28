import requests


# make a request to the following url

url = "http://127.0.0.1:8000/get_song/test1/enemy.flac"

# get the response
response = requests.get(url)

# print the response
print(response.json())


# the response is a dictionary representing a song file
# {"song_name": song_path, "song_bytes": f.read().hex()}

# the song_bytes is a hex string of the bytes of the song file
# to get the bytes, use bytes.fromhex(response.json()["song_bytes"])
# write the bytes to a file
with open(response.json()["song_name"], "wb") as f:
    f.write(bytes.fromhex(response.json()["song_bytes"]))