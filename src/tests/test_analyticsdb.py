import pytest
import sys
import os
import sqlite3
import random
import zipfile

# add the parent directory to the path
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "core")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "backups")))
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "databases")))
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "soundfiles")))

import config
import analyticsdb
import songparser

# unzip the test database in config.ZIPPED_DATABASE_TEST_PATH1

dbHelper = analyticsdb.AnalyticsDBHandler(debug_path=config.DATABASE_TEST_PATH)


def setup_prezipped_db():
    with zipfile.ZipFile(config.ZIPPED_DATABASE_TEST_PATH1, "r") as zip_ref:
        zip_ref.extractall(os.path.dirname(config.DATABASE_TEST_PATH))


song_table_data = config.song_table_data.copy()


def setup_test_db1():
    # create a test database
    # dbHelper.clear_all_tables()
    dbHelper.create_all_tables()
    dbHelper.populate_database()

    # lets make some example playlists
    # def insert_playlist(self, playlist_name: str, playlist_desc: str, created_dt: str) -> bool:
    dbHelper.insert_playlist(
        "test_playlist_1", "test playlist 1 description", "2020-01-01")
    dbHelper.insert_playlist(
        "favorites", "test playlist 2 description", "2020-01-02")

    # lets make some example songs
    # def insert_playlist_song(self, playlist_name: str, song_id: str) -> bool:

    # THESE NEED TO BE COMMENTED OTHERWISE THEY WILL RE-INSERT THE SAME SONGS. UNCOMMENT TO RE-CREATE DATABASE

    dbHelper.insert_playlist_song("test_playlist_1", "23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956")
    dbHelper.insert_playlist_song("test_playlist_1", "b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0")
    dbHelper.insert_playlist_song("test_playlist_1", "4c1e39f575afeb262287c300338256d3b4e67d7bd5e4d431bb3aa67f7be84daa")
    dbHelper.insert_playlist_song("favorites","23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956")
    dbHelper.insert_playlist_song("favorites", "b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0")
    dbHelper.insert_playlist_song("favorites", "89661c6cc19c7f25ecb91d937d175394170672277527282ea7cec71e412c84ef")

    # lets add some example plays

    # lets get the song table to look at the song_title, song_primary_artist, filesize

    # def get_play_information_from_song_id(self, song_id: str) -> Dict[str, int]:

    songs_info = []
    # get all song_ids
    songs = dbHelper.get_all_songs()
    for song in songs:
        song_id = song["song_id"]
        song_info = dbHelper.get_play_information_from_song_id(song_id)
        songs_info.append(song_info)

    # add some plays using this info
    # def insert_play(self, song_title: str, song_primary_artist: str, filesize: int, start_dt: str, end_dt: str) -> bool:
    for i in range(0, 50):
        # randomly pick a song
        song = random.choice(songs_info)
        # randomly pick a start time
        start_time = random.randint(0, 1000000000)
        # randomly pick an endtime up to 10 minutes after the start time
        end_time = random.randint(start_time, start_time + 600)
        # insert the play
        dbHelper.insert_play(
            song["song_title"], song["song_primary_artist"], song["filesize"], start_time, end_time, song["song_id"])


def test_vital_paths():
    # check if ../analyticsdb ../backups ../logs exist
    assert os.path.exists(os.path.dirname(config.DATABASE_PATH))
    assert os.path.exists(config.BACKUPS_PATH)
    assert os.path.exists(os.path.dirname(config.LOGGING_FILENAME))
    assert os.path.exists(os.path.dirname(config.ZIPPED_DATABASE_TEST_PATH1))

# --------------------------------------------------------------------------------
#                          TESTING CREATE TABLES
# --------------------------------------------------------------------------------


def test_create_songs_table():
    try:
        dbHelper.clear_all_tables()
    except:
        pass
    dbHelper.create_songs_table()
    # get all the columns from the songs table
    columns = dbHelper.get_all_columns_from_table("songs")
    assert columns == ['song_id', 'filepath', 'filesize', 'padding', 'album_artwork_bit_depth', 'album_artwork_colors', 'album_artwork_height', 'album_artwork_width', 'bit_depth', 'bitrate', 'channels', 'duration',
                       'sample_rate', 'album', 'barcode', 'date_created', 'disc_number', 'disc_total', 'isrc', 'itunesadvisory', 'length', 'publisher', 'rating', 'title', 'track_number', 'track_total', 'source', 'main_artist']


def test_create_plays_table():
    dbHelper.create_plays_table()
    # get all the columns from the plays table
    columns = dbHelper.get_all_columns_from_table("plays")
    assert columns == ['play_id', 'song_title',
                       'song_primary_artist', 'filesize', 'start_dt', 'end_dt', 'song_id']


def test_create_playlists_table():
    dbHelper.create_playlists_table()
    # get all the columns from the playlists table
    columns = dbHelper.get_all_columns_from_table("playlists")
    assert columns == ['playlist_id', 'playlist_name',
                       'playlist_desc', 'created_dt']


def test_create_playlist_songs_table():
    dbHelper.create_playlists_songs_table()
    # get all the columns from the playlist_songs table
    columns = dbHelper.get_all_columns_from_table("playlists_songs")
    assert columns == ['playlist_id', 'song_id', 'added_dt']


def test_create_song_artists_table():
    dbHelper.create_song_artists_table()
    # get all the columns from the song_artists table
    columns = dbHelper.get_all_columns_from_table("song_artists")
    assert columns == ['artist_name', 'song_id', 'dt_added']


def test_create_album_artists_table():
    dbHelper.create_album_artists_table()
    # get all the columns from the album_artists table
    columns = dbHelper.get_all_columns_from_table("album_artists")
    assert columns == ['artist_name', 'song_id', 'dt_added']


def test_create_composers_table():
    dbHelper.create_composers_table()
    # get all the columns from the composers table
    columns = dbHelper.get_all_columns_from_table("composers")
    assert columns == ['composer_name', 'song_id', 'dt_added']


def test_create_genres_table():
    dbHelper.create_genres_table()
    # get all the columns from the genres table
    columns = dbHelper.get_all_columns_from_table("genres")
    assert columns == ['genre_name', 'song_id', 'dt_added']

# --------------------------------------------------------------------------------
#                          TESTING CLEAR TABLES
# --------------------------------------------------------------------------------


def test_clear_songs_table():
    dbHelper.clear_songs_table()
    # get all the columns from the songs table
    try:
        columns = dbHelper.get_all_columns_from_table("songs")
    except Exception as e:
        assert True


def test_clear_plays_table():
    dbHelper.clear_plays_table()
    # get all the columns from the plays table
    try:
        columns = dbHelper.get_all_columns_from_table("plays")
    except Exception as e:
        assert True


def test_clear_playlists_table():
    dbHelper.clear_playlists_table()
    # get all the columns from the playlists table
    try:
        columns = dbHelper.get_all_columns_from_table("playlists")
    except Exception as e:
        assert True


def test_clear_playlist_songs_table():
    dbHelper.clear_playlists_songs_table()
    # get all the columns from the playlist_songs table
    try:
        columns = dbHelper.get_all_columns_from_table("playlists_songs")
    except Exception as e:
        assert True


def test_clear_song_artists_table():
    dbHelper.clear_song_artists_table()
    # get all the columns from the song_artists table
    try:
        columns = dbHelper.get_all_columns_from_table("song_artists")
    except Exception as e:
        assert True


def test_clear_album_artists_table():
    dbHelper.clear_album_artists_table()
    # get all the columns from the album_artists table
    try:
        columns = dbHelper.get_all_columns_from_table("album_artists")
    except Exception as e:
        assert True


def test_clear_composers_table():
    dbHelper.clear_composers_table()
    # get all the columns from the composers table
    try:
        columns = dbHelper.get_all_columns_from_table("composers")
    except Exception as e:
        assert True


def test_clear_genres_table():
    dbHelper.clear_genres_table()
    # get all the columns from the genres table
    try:
        columns = dbHelper.get_all_columns_from_table("genres")
    except Exception as e:
        assert True

# --------------------------------------------------------------------------------
#                          TESTING INSERTS AND RETRIEVALS
# --------------------------------------------------------------------------------

    # def insert_play(self, song_title: str, song_primary_artist: str, filesize: int, start_dt: str, end_dt: str) -> bool:
    # def insert_playlist(self, playlist_name: str, playlist_desc: str, created_dt: str) -> bool:
    # def insert_playlist_song(self, playlist_name: str, song_id: str) -> bool:
    # def insert_song(self, **kwargs) -> int:
    # def insert_album_artist(self, artist_name, song_id) -> bool:
    # def insert_song_artist(self, artist_name, song_id) -> bool:
    # def insert_composer(self, composer_name, song_id) -> bool:
    # def insert_genre(self, genre_name, song_id) -> bool:


def test_insert_song():
    dbHelper.create_all_tables()
    song_id = dbHelper.insert_song(**song_table_data)
    song_data = dbHelper.get_song_by_id(song_id)
    assert song_data == song_table_data


def test_insert_play():
    play_table_data = {
        "song_title": "test song",
        "song_primary_artist": "test artist",
        "filesize": 123456,
        "start_dt": "2020-01-01 00:00:00",
        "end_dt": "2020-01-01 00:00:00",
        "song_id": song_table_data["song_id"]
    }
    play_id = dbHelper.insert_play(play_table_data["song_title"], play_table_data["song_primary_artist"],
                                   play_table_data["filesize"], play_table_data["start_dt"], play_table_data["end_dt"], play_table_data["song_id"])
    
    play_data = dbHelper.get_play_by_id(play_id)
    play_table_data["play_id"] = play_id
    assert play_data == play_table_data

def test_insert_playlist():
    playlist_table_data = {
        "playlist_name": "test playlist",
        "playlist_desc": "test playlist description",
        "created_dt": "2020-01-01 00:00:00"
    }
    playlist_id = dbHelper.insert_playlist(playlist_table_data["playlist_name"], playlist_table_data["playlist_desc"],
                                           playlist_table_data["created_dt"])
    playlist_data = dbHelper.get_playlist_by_id(playlist_id)
    playlist_table_data["playlist_id"] = playlist_id
    assert playlist_data == playlist_table_data

def test_insert_playlist_song():
    # get all playlists
    playlists = dbHelper.get_all_playlist_names()
    # get all songs
    songs = dbHelper.get_all_songs()
    # insert a playlist song
    playlist_song_id = dbHelper.insert_playlist_song(playlists[0], songs[0]["song_id"])
    # get the playlist song
    playlist_songs = dbHelper.get_songs_in_playlist(playlists[0])
    playlist_songs[0]["song_id"] = songs[0]["song_id"]
    # print(playlist_songs[0])
    # print(songs[0])
    assert playlist_songs[0] == songs[0]

def test_insert_album_artist():
    # get all songs
    songs = dbHelper.get_all_songs()
    # insert an album artist
    dbHelper.insert_album_artist("test artist", songs[0]["song_id"])
    # get the album artist
    album_artists = dbHelper.get_all_album_artists()
    assert album_artists[0] == "test artist"

def test_insert_song_artist():
    # get all songs
    songs = dbHelper.get_all_songs()
    # insert a song artist
    dbHelper.insert_song_artist("test artist", songs[0]["song_id"])
    # get the song artist
    song_artists = dbHelper.get_all_song_artists()
    assert song_artists[0] == "test artist"

def test_insert_composer():
    # get all songs
    songs = dbHelper.get_all_songs()
    # insert a composer
    dbHelper.insert_composer("test composer", songs[0]["song_id"])
    # get the composer
    composers = dbHelper.get_all_composers()
    assert composers[0] == "test composer"

def test_insert_genre():
    # get all songs
    songs = dbHelper.get_all_songs()
    # insert a genre
    dbHelper.insert_genre("test genre", songs[0]["song_id"])
    # get the genre
    genres = dbHelper.get_all_genres()
    assert genres[0] == "test genre"

def test_populate_db():
    dbHelper.clear_all_tables()
    dbHelper.create_all_tables()
    testsongs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "testsongs"))
    dbHelper.populate_database(soundfiles_path=testsongs_path)

    
    # check the following tables:
    # songs: 10 records with following song ids
    # 23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956
    # b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0
    # 4c1e39f575afeb262287c300338256d3b4e67d7bd5e4d431bb3aa67f7be84daa
    # 35dba2f69761f9d681662b6364974043261c9672f34aba7d8cb5a821d10ccea6
    # cfc3b7e098d8105ffd5eab92bbe4b01bd5833029b086cbcc72ca25f1f6d3fec0
    # 5f3012eccf122cf6e68243c57497d87498b01592e016d00c73143f50d2d45200
    # 3dd9d2253c8156c8e66c6ed6acf04cb9603541618eafe39b4e8b28541d7059f5
    # 82fef55bfe4e4ae4b3fe598acafd499cc22f7c47e12a54f850c13c8cca1a8471
    # 0915edd1c75ef33839528377eea8265cddbca4eb7722dc92d74e9cd020497946
    # 643a7837128bc5b932cf8b9dd6391d865801a492bfb361500ab190cc4abce226

    songs = dbHelper.get_all_songs()
    assert len(songs) == 10
    assert songs[0]["song_id"] == "23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956"
    assert songs[1]["song_id"] == "b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0"
    assert songs[2]["song_id"] == "4c1e39f575afeb262287c300338256d3b4e67d7bd5e4d431bb3aa67f7be84daa"
    assert songs[3]["song_id"] == "35dba2f69761f9d681662b6364974043261c9672f34aba7d8cb5a821d10ccea6"
    assert songs[4]["song_id"] == "cfc3b7e098d8105ffd5eab92bbe4b01bd5833029b086cbcc72ca25f1f6d3fec0"
    assert songs[5]["song_id"] == "5f3012eccf122cf6e68243c57497d87498b01592e016d00c73143f50d2d45200"
    assert songs[6]["song_id"] == "3dd9d2253c8156c8e66c6ed6acf04cb9603541618eafe39b4e8b28541d7059f5"
    assert songs[7]["song_id"] == "82fef55bfe4e4ae4b3fe598acafd499cc22f7c47e12a54f850c13c8cca1a8471"
    assert songs[8]["song_id"] == "0915edd1c75ef33839528377eea8265cddbca4eb7722dc92d74e9cd020497946"
    assert songs[9]["song_id"] == "643a7837128bc5b932cf8b9dd6391d865801a492bfb361500ab190cc4abce226"
    
    # song_artists: 13 records with the following people appearing n times:
    # MIYAVI: 8
    # Sky-Hi: 1
    # Martin Garrix: 1
    # Macklemore: 1
    # Fall Out Boy: 1
    # Rag'n'Bone Man: 1

    song_artists = dbHelper.get_all_song_artists(no_duplicates=False)
    assert len([x for x in song_artists if x == "MIYAVI"]) == 8
    assert len([x for x in song_artists if x == "Sky-Hi"]) == 1
    assert len([x for x in song_artists if x == "Martin Garrix"]) == 1
    assert len([x for x in song_artists if x == "Macklemore"]) == 1
    assert len([x for x in song_artists if x == "Fall Out Boy"]) == 1
    assert len([x for x in song_artists if x == "Rag'n'Bone Man"]) == 1

    # album_artists: 12 records with the following people appearing n times:
    # MIYAVI: 8
    # Martin Garrix: 1
    # Macklemore: 1
    # Fall Out Boy: 1
    # Rag'n'Bone Man: 1

    album_artists = dbHelper.get_all_album_artists(no_duplicates=False)
    assert len([x for x in album_artists if x == "MIYAVI"]) == 8
    assert len([x for x in album_artists if x == "Martin Garrix"]) == 1
    assert len([x for x in album_artists if x == "Macklemore"]) == 1
    assert len([x for x in song_artists if x == "Fall Out Boy"]) == 1
    assert len([x for x in song_artists if x == "Rag'n'Bone Man"]) == 1

    genres = dbHelper.get_all_genres(no_duplicates=False)

    # ['Rock', 'Rock', 'Electro', 'Techno/House', 'Dance', 'Pop', 'International Pop', 'Rock', 'Rock', 'Rock', 'Rock', 'Alternative; Indie Pop; Indie Rock', 'Rock', 'Rock', 'Rock']

    assert len([x for x in genres if x == "Rock"]) == 9
    assert len([x for x in genres if x == "Electro"]) == 1
    assert len([x for x in genres if x == "Techno/House"]) == 1
    assert len([x for x in genres if x == "Dance"]) == 1
    assert len([x for x in genres if x == "Pop"]) == 1
    assert len([x for x in genres if x == "International Pop"]) == 1
    assert len([x for x in genres if x == "Alternative; Indie Pop; Indie Rock"]) == 1

    composers = dbHelper.get_all_composers(no_duplicates=False)

#     ['Sky-Hi', 'MIYAVI', 'Lenny Skolnik', 'Jonny Litten', 'Miyavi', 'Martijn Garritsen', 'Brian Lee', 'Jaramye Daniels', 
# 'Giorgio Tuinfort', 'Benjamin Hammond Haggerty', 'Andrew Ramsey', 'Shannon Sanders', 'MIYAVI', 'MIYAVI', 'Andrew Ramsey', 'Shannon Sanders', 'Lenny Skolnik', 'Ilan Kidron', 'MIYAVI', 'MIYAVI', 'Lenny Skolnik', 'Seann Bowe', 'MIYAVI', 'Lenny Skolnik', 'Ilan Kidron', 'Doc Brittain', 'RAS', 'Lalo Schifrin']

    assert len([x for x in composers if x == "Sky-Hi"]) == 1
    assert len([x for x in composers if x == "MIYAVI"]) == 6
    assert len([x for x in composers if x == "Lenny Skolnik"]) == 4
    assert len([x for x in composers if x == "Ilan Kidron"]) == 2
    assert len([x for x in composers if x == "Lalo Schifrin"]) == 1
    assert len([x for x in composers if x == "RAS"]) == 1
    assert len([x for x in composers if x == "Doc Brittain"]) == 1
    assert len([x for x in composers if x == "Seann Bowe"]) == 1
    assert len([x for x in composers if x == "Martijn Garritsen"]) == 1
    assert len([x for x in composers if x == "Brian Lee"]) == 1
    assert len([x for x in composers if x == "Jaramye Daniels"]) == 1
    assert len([x for x in composers if x == "Giorgio Tuinfort"]) == 1
    assert len([x for x in composers if x == "Benjamin Hammond Haggerty"]) == 1
    assert len([x for x in composers if x == "Andrew Ramsey"]) == 2
    assert len([x for x in composers if x == "Shannon Sanders"]) == 2
    assert len([x for x in composers if x == "Jonny Litten"]) == 1
    assert len([x for x in composers if x == "Miyavi"]) == 1
    assert len([x for x in composers if x == "Lenny Skolnik"]) == 4

# --------------------------------------------------------------------------------
#                          COMPLICATED TESTING - RETRIEVALS
# --------------------------------------------------------------------------------

def test_get_all_songs():
    setup_prezipped_db()
    songs = dbHelper.get_all_songs()
    assert len(songs) == 18

def test_get_song_artists_of_song():
    songs = dbHelper.get_all_songs()

    check_dict = {
        0: ["MIYAVI", "Sky-Hi"],
        1: ["MIYAVI"],
        2: ["Martin Garrix", "Macklemore", "Fall Out Boy"],
        3: ["MIYAVI"],
        4: ["MIYAVI"],
        5: ["MIYAVI"],
        6: ["Rag'n'Bone Man"],
        7: ["MIYAVI"],
        8: ["MIYAVI"],
        9: ["MIYAVI"],
        10: [],
        11: ["Imagine Dragons", "Arcane", "League Of Legends"],
        12: [],
        13: ["Frank Ocean"],
        14: ["Hudson Mohawke"],
        15: ["Lil Uzi Vert"],
        16: ["Diggy-MO'"],
        17: ["Steve Lacy"]

    }
    for idx, song in enumerate(songs):
        song_artists = dbHelper.get_song_artists_of_song(song["song_id"])
        assert song_artists == check_dict[idx]

def test_get_album_artists_of_song():
    songs = dbHelper.get_all_songs()

    check_dict = {
        0: ["MIYAVI"],
        1: ["MIYAVI"],
        2: ["Martin Garrix", "Macklemore", "Fall Out Boy"],
        3: ["MIYAVI"],
        4: ["MIYAVI"],
        5: ["MIYAVI"],
        6: ["Rag'n'Bone Man"],
        7: ["MIYAVI"],
        8: ["MIYAVI"],
        9: ["MIYAVI"],
        10: [],
        11: ["Imagine Dragons", "Arcane", "League Of Legends"],
        12: [],
        13: ["Frank Ocean"],
        14: ["Hudson Mohawke"],
        15: ["Lil Uzi Vert"],
        16: [],
        17: ["Steve Lacy"]

    }

    for idx, song in enumerate(songs):
        album_artists = dbHelper.get_album_artists_of_song(song["song_id"])
        assert album_artists == check_dict[idx]

def test_get_composers_of_song():
    songs = dbHelper.get_all_songs()
    check_dict = {
        0: ['Sky-Hi', 'MIYAVI', 'Lenny Skolnik', 'Jonny Litten'],
        1: ['Miyavi'],
        2: ['Martijn Garritsen', 'Brian Lee', 'Jaramye Daniels', 'Giorgio Tuinfort', 'Benjamin Hammond Haggerty'],
        3: ['Andrew Ramsey', 'Shannon Sanders', 'MIYAVI'],
        4: ['MIYAVI', 'Andrew Ramsey', 'Shannon Sanders'],
        5: ['Lenny Skolnik', 'Ilan Kidron', 'MIYAVI'],
        6: [],
        7: ['MIYAVI', 'Lenny Skolnik', 'Seann Bowe'],
        8: ['MIYAVI', 'Lenny Skolnik', 'Ilan Kidron', 'Doc Brittain', 'RAS'],
        9: ['Lalo Schifrin'],
        10: [],
        11: ['Dan Reynolds', 'Wayne Sermon', 'Ben McKee', 'Daniel Platzman', 'Robin Fredriksson', 'Mattias Larsson', 'Justin Tranter', 'Destin Route'],
        12: [],
        13: ['Taylor Johnson', 'James Ho', 'Lonnie Breaux'],
        14: [],
        15: [],
        16: [],
        17: []
    }
    for idx, song in enumerate(songs):
        composers = dbHelper.get_composers_of_song(song["song_id"])
        assert composers == check_dict[idx]
    
def test_get_genres_of_song():
    songs = dbHelper.get_all_songs()
    check_dict = {
        0: ['Rock'],
        1: ['Rock'],
        2: ['Electro', 'Techno/House', 'Dance', 'Pop', 'International Pop', 'Rock'],
        3: ['Rock'],
        4: ['Rock'],
        5: ['Rock'],
        6: ['Alternative; Indie Pop; Indie Rock'],
        7: ['Rock'],
        8: ['Rock'],
        9: ['Rock'],
        10: [],
        11: ['Alternative'],
        12: [],
        13: ['Pop'],
        14: ['Alternative', 'Electro'],
        15: ['Rap/Hip Hop'],
        16: ['Anime'],
        17: ['R&B']
    }
    for idx, song in enumerate(songs):
        genres = dbHelper.get_genres_of_song(song["song_id"])
        assert genres == check_dict[idx]

def test_get_songs_in_playlist():
    playlists = dbHelper.get_all_playlist_names()
    playlist1 = playlists[0]
    playlist2 = playlists[1]

    songs1 = dbHelper.get_songs_in_playlist(playlist1)
    songs2 = dbHelper.get_songs_in_playlist(playlist2)

    assert songs1[0]["song_id"] == "23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956"
    assert songs1[1]["song_id"] == "b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0"
    assert songs1[2]["song_id"] == "4c1e39f575afeb262287c300338256d3b4e67d7bd5e4d431bb3aa67f7be84daa"

    assert songs2[0]["song_id"] == "23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956"
    assert songs2[1]["song_id"] == "b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0"
    assert songs2[2]["song_id"] == "89661c6cc19c7f25ecb91d937d175394170672277527282ea7cec71e412c84ef"

def test_get_songs_in_album():
    albums = dbHelper.get_all_albums()
    # print(albums)


# [{'song_title': 'Gemstone', 'song_primary_artist': 'MIYAVI', 'filesize': 34815481, 'song_id': '23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956'}]
# [{'song_title': 'Loaded', 'song_primary_artist': 'Lil Uzi Vert', 'filesize': 21073949, 'song_id': 'cd0c07ef5178049c30cc632002e39000f138eb35ebd300b3e3b349da05670eff'}]
# [{'song_title': 'Bakusou Yumeuta (爆走夢歌)', 'song_primary_artist': "Diggy-MO'", 'filesize': 8364414, 'song_id': 'c8d3a45d28c2bdc14b35d51837c980ca3d878127ca0d0ebfbe337517f489a3ab'}]
# []
# [{'song_title': 'Summer Days (feat. Macklemore & Patrick Stump of Fall Out Boy)', 'song_primary_artist': 'Martin Garrix', 'filesize': 18447881, 'song_id': '4c1e39f575afeb262287c300338256d3b4e67d7bd5e4d431bb3aa67f7be84daa'}]
# [{'song_title': 'Enemy (From the series Arcane League of Legends)', 'song_primary_artist': 'Imagine Dragons', 'filesize': 21495795, 'song_id': 'f2ee683a57fb292b2af911d3522996c3a4497e03cb509b9b62efbd0557c6d582'}]
# [{'song_title': 'Pyramids', 'song_primary_artist': 'Frank Ocean', 'filesize': 64746295, 'song_id': '0b8a3f9714d616e00b59c451b21783bdb4e964330fd6f112fab5973bc3868f05'}]
# [{'song_title': 'Cbat', 'song_primary_artist': 'Hudson Mohawke', 'filesize': 2973802, 'song_id': '673738b05a45d5de00ae08e007372e45dc941dcd2d386044eaae3675e928854d'}]
# [{'song_title': 'Thats No Fun', 'song_primary_artist': 'Steve Lacy', 'filesize': 18688972, 'song_id': '89661c6cc19c7f25ecb91d937d175394170672277527282ea7cec71e412c84ef'}]
# [{'song_title': 'Human', 'song_primary_artist': "Rag'n'Bone Man", 'filesize': 21857519, 'song_id': '3dd9d2253c8156c8e66c6ed6acf04cb9603541618eafe39b4e8b28541d7059f5'}]
# [{'song_title': 'What A Wonderful World (Day 2 Mix)', 'song_primary_artist': 'MIYAVI', 'filesize': 30181873, 'song_id': 'b87519d8ede9ab4e642bbe41815cbaf2ddb5245e5b23052a966808ef908e50b0'}, {'song_title': 'Let Go', 'song_primary_artist': 'MIYAVI', 'filesize': 19641836, 'song_id': '35dba2f69761f9d681662b6364974043261c9672f34aba7d8cb5a821d10ccea6'}, {'song_title': 'The Others', 'song_primary_artist': 'MIYAVI', 'filesize': 27868010, 'song_id': 'cfc3b7e098d8105ffd5eab92bbe4b01bd5833029b086cbcc72ca25f1f6d3fec0'}, {'song_title': 'Afraid To Be Cool', 'song_primary_artist': 'MIYAVI', 'filesize': 28612566, 'song_id': '5f3012eccf122cf6e68243c57497d87498b01592e016d00c73143f50d2d45200'}, {'song_title': 'Fire Bird', 'song_primary_artist': 'MIYAVI', 'filesize': 26848748, 'song_id': '82fef55bfe4e4ae4b3fe598acafd499cc22f7c47e12a54f850c13c8cca1a8471'}, {'song_title': 'Long Nights', 'song_primary_artist': 'MIYAVI', 'filesize': 32307426, 'song_id': '0915edd1c75ef33839528377eea8265cddbca4eb7722dc92d74e9cd020497946'}, {'song_title': 'Mission: Impossible Theme', 'song_primary_artist': 'MIYAVI', 'filesize': 38018706, 'song_id': '643a7837128bc5b932cf8b9dd6391d865801a492bfb361500ab190cc4abce226'}]

    check_dict = [
        {'artist_name': 'Imagine Dragons', 'album_name': 'Enemy (from the series Arcane League of Legends)', 'song_count': 1},
        {'artist_name': 'MIYAVI', 'album_name': 'Samurai Sessions Vol.2', 'song_count': 1},
        {'artist_name': "Diggy-MO'", 'album_name': 'Bakusou Yumeuta', 'song_count': 1},
        {'artist_name': 'Hudson Mohawke', 'album_name': 'Satin Panthers', 'song_count': 1},
        {'artist_name': 'Unknown Artist', 'album_name': None, 'song_count': 0},
        {'artist_name': 'Frank Ocean', 'album_name': 'channel ORANGE', 'song_count': 1},
        {'artist_name': 'Martin Garrix', 'album_name': 'Summer Days (feat. Macklemore & Patrick Stump of Fall Out Boy)', 'song_count': 1},
        {'artist_name': 'Steve Lacy', 'album_name': 'The Lo-Fis', 'song_count': 1},
        {'artist_name': 'MIYAVI', 'album_name': 'All Time Best "Day 2"', 'song_count': 7},
        {'artist_name': "Rag'n'Bone Man", 'album_name': 'Human (Deluxe)', 'song_count': 1}

    ]



    for idx, album in enumerate(albums):
        songs_in_album = dbHelper.get_songs_in_album(album['album_name'], album['artist_name'])
        for dic in check_dict:
            if dic['artist_name'] == album['artist_name'] and dic['album_name'] == album['album_name']:
                assert len(songs_in_album) == dic['song_count']

def test_get_songs_in_album_artist():
    artists = dbHelper.get_all_album_artists()

    check_dict = {
        "Martin Garrix": 1,
        "Macklemore": 1,
        "Fall Out Boy": 1,
        "Rag'n'Bone Man": 1,
        "Imagine Dragons": 1,
        "Arcane": 1,
        "League Of Legends": 1,
        "Frank Ocean": 1,
        "Hudson Mohawke": 1,
        "Lil Uzi Vert": 1,
        "Steve Lacy": 1,
        "MIYAVI": 8
    }
    for idx, artist in enumerate(artists):
        songs_in_artist = dbHelper.get_songs_in_album_artist(artist)
        assert len(songs_in_artist) == check_dict[artist]

def test_get_songs_in_song_artist():
    artists = dbHelper.get_all_song_artists()
    check_dict = {
        "Martin Garrix": 1,
        "Macklemore": 1,
        "Fall Out Boy": 1,
        "Rag'n'Bone Man": 1,
        "Imagine Dragons": 1,
        "Arcane": 1,
        "League Of Legends": 1,
        "Frank Ocean": 1,
        "Hudson Mohawke": 1,
        "Lil Uzi Vert": 1,
        "Steve Lacy": 1,
        "MIYAVI": 8,
        "Diggy-MO'": 1,
        "Sky-Hi": 1
    }

    for idx, artist in enumerate(artists):
        songs_in_artist = dbHelper.get_songs_in_song_artist(artist)
        assert len(songs_in_artist) == check_dict[artist]

def test_get_songs_in_composer():
    composers = dbHelper.get_all_composers()
    

    check_dict = {
        "Sky-Hi": 1,
        "MIYAVI": 6,
        "Lenny Skolnik": 4,
        "Jonny Litten": 1,
        "Miyavi": 1,
        "Martijn Garritsen": 1,
        "Brian Lee": 1,
        "Jaramye Daniels": 1,
        "Giorgio Tuinfort": 1,
        "Benjamin Hammond Haggerty": 1,
        "Andrew Ramsey": 2,
        "Shannon Sanders": 2,
        "Ilan Kidron": 2,
        "Seann Bowe": 1,
        "Doc Brittain": 1,
        "RAS": 1,
        "Lalo Schifrin": 1,
        "Dan Reynolds": 1,
        "Wayne Sermon": 1,
        "Ben McKee": 1,
        "Daniel Platzman": 1,
        "Robin Fredriksson": 1,
        "Mattias Larsson": 1,
        "Justin Tranter": 1,
        "Destin Route": 1,
        "Taylor Johnson": 1,
        "James Ho": 1,
        "Lonnie Breaux": 1
    }

    for idx, composer in enumerate(composers):
        songs_in_composer = dbHelper.get_songs_in_composer(composer)
        assert len(songs_in_composer) == check_dict[composer]

def test_get_songs_in_genre():
    genres = dbHelper.get_all_genres()

    check_dict = {
        "Rock": 9,
        "Electro": 2,
        "Techno/House": 1,
        "Dance": 1,
        "Pop": 2,
        "International Pop": 1,
        "Alternative; Indie Pop; Indie Rock": 1,
        "Alternative": 2,
        "Rap/Hip Hop": 1,
        "Anime": 1,
        "R&B": 1
    }
    for idx, genre in enumerate(genres):
        songs_in_genre = dbHelper.get_songs_in_genre(genre)
        assert len(songs_in_genre) == check_dict[genre]


if __name__ == "__main__":
    # test_clear_songs_table()
    # setup_test_db1()
    test_populate_db()
