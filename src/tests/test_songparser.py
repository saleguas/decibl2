
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

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "testsongs2")))

import songparser
import analyticsdb
import config

def test_file_to_hash():
    filepath = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "testsongs", "02 - Gemstone.flac"))
    hash = songparser.file_to_hash(filepath)
    assert hash == "23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956"


def test_song_file_FLAC():
    filepath = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "testsongs2", "02 - Gemstone.flac"))
    song = songparser.SongFileFLAC()
    song.load_file(filepath)

    song_test = {'song_id': '23fb2258052511a4d07bc555a1b45a41fbd8da0f3ec4a887c9c7282351672956', 'filepath': 'c:\\Users\\drale\\Documents\\GitHub\\decibl-docker\\src\\tests\\testsongs\\02 - Gemstone.flac', 'main_artist': 'MIYAVI', 'filesize': 34815481, 'padding': 8192, 'album_artwork_bit_depth': 24, 'album_artwork_colors': 0, 'album_artwork_height': 800, 'album_artwork_width': 800, 'bit_depth': 16, 'bitrate': 1140600.0330232815,
                 'channels': 2, 'duration': 242.25333333333333, 'sample_rate': 44100, 'album': 'Samurai Sessions Vol.2', 'barcode': '602567021407', 'date_created': '2017-11-08', 'disc_number': '1', 'disc_total': '1', 'isrc': 'JPPO01705093', 'itunesadvisory': '0', 'length': '242000', 'publisher': 'UNIVERSAL MUSIC LLC', 'rating': '100', 'title': 'Gemstone', 'track_number': '2', 'track_total': '11', 'source': 'Deezer'}
    
    album_artist_test = ['MIYAVI']
    song_artist_test = ['MIYAVI', 'Sky-Hi']
    composer_test = ['Sky-Hi', 'MIYAVI', 'Lenny Skolnik', 'Jonny Litten']
    genre_test = ['Rock']

    album_artist = song.get_album_artist_data()
    song_artist = song.get_song_artist_data()
    composer = song.get_composer_data()
    genre = song.get_genre_data()

    for key in song_test.keys():
        if key == "filepath":
            continue
        assert song_test[key] == song.get_song_table_data()[key]

    assert album_artist == album_artist_test
    assert song_artist == song_artist_test
    assert composer == composer_test
    assert genre == genre_test

def test_song_file_MP3():
    filepath = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "testsongs2", "example.mp3"))
    song = songparser.SongFileMP3()
    song.load_file(filepath)

    # print(song.get_song_table_data())
    # print(song.get_album_artist_data())
    # print(song.get_song_artist_data())
    # print(song.get_composer_data())
    # print(song.get_genre_data())

    song_test = {'song_id': 'c8d3a45d28c2bdc14b35d51837c980ca3d878127ca0d0ebfbe337517f489a3ab', 'filepath': 'c:\\Users\\drale\\Documents\\GitHub\\decibl-docker\\src\\tests\\testsongs2\\example.mp3', 'main_artist': "Diggy-MO'", 'filesize': 8364414, 'padding': None, 'album_artwork_bit_depth': None, 'album_artwork_colors': None, 'album_artwork_height': 1200, 'album_artwork_width': 1200, 'bit_depth': None, 'bitrate': 244599.11789652248, 'channels': 2, 'duration': 235.8136386428074, 'sample_rate': 44100, 'album': 'Bakusou Yumeuta', 'barcode': None, 'date_created': '2008', 'disc_number': '1', 'disc_total': None, 'isrc': None, 'itunesadvisory': None, 'length': None, 'publisher': None, 'rating': None, 'title': 'Bakusou Yumeuta (爆走夢歌)', 'track_number': '1', 'track_total': None, 'source': None}

    album_artist_test = None
    song_artist_test = ["Diggy-MO'"]
    composer_test = None
    genre_test = ['Anime']

    album_artist = song.get_album_artist_data()
    song_artist = song.get_song_artist_data()
    composer = song.get_composer_data()
    genre = song.get_genre_data()

    for key in song_test.keys():
        if key == "filepath":
            continue
        assert song_test[key] == song.get_song_table_data()[key]

    assert album_artist == album_artist_test
    assert song_artist == song_artist_test
    assert composer == composer_test
    assert genre == genre_test




# test_song_file_MP3()