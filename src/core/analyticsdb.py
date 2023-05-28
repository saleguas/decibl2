import datetime
import os
import sys
import sqlite3
from typing import Dict, List
import zipfile
# add current file to system path

import logging
import config
import songparser
from fastapi import HTTPException
from progress.bar import Bar


# log a message
logging.info("Loading database module")

# Make Database class to hold all the data analytics
# This will be used to create and manage the database of the users activity.
# (there's lots of data in the songs table so I'm not going to put it in the graphic)



class AnalyticsDBHandler:
    """Class to handle all the data analytics, especially stuff like creating tables, making backups, etc."""

    # CONSTRUCTOR

    def __init__(self, debug_path=None) -> None:
        """
        __init__ Initialize the database handler. Creates the database at the path specified in config.py

        Args:
            debug_path (str, optional): Path to the database for debug. Defaults to None.
        """
        if debug_path is None:
            self.conn = sqlite3.connect(config.DATABASE_PATH)
        else:
            self.conn = sqlite3.connect(debug_path)

    # ------------------------------------------------------------------------------------------------------------
    # --------------------------------------------------------------------------------------------
    #                                    Error Handling
    # --------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    def raise_table_not_created(self, table, details):
        err = "Unable to create "+table+" table: "+details
        logging.info(err)
        raise HTTPException(status_code=400, detail=err)

    # ------------------------------------------------------------------------------------------------------------
    # --------------------------------------------------------------------------------------------
    #                                    CREATE TABLES
    # --------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    def create_songs_table(self):
        """
        create_songs_table Creates the songs table in the database.
        """
        try:
            logging.info("Creating songs table")
            cursor = self.conn.cursor()
            # This is going to be a LOT of data, make a table with the following:
            # Create the table
            cursor.execute("""CREATE TABLE IF NOT EXISTS songs (
                song_id TEXT PRIMARY KEY,
                filepath TEXT,
                filesize BIGINT,
                padding INTEGER,
                album_artwork_bit_depth INTEGER,
                album_artwork_colors INTEGER,
                album_artwork_height INTEGER,
                album_artwork_width INTEGER,
                bit_depth INTEGER,
                bitrate INTEGER,
                channels INTEGER,
                duration INTEGER,
                sample_rate INTEGER,
                album TEXT,
                barcode TEXT,
                date_created TEXT,
                disc_number INTEGER,
                disc_total INTEGER,
                isrc TEXT,
                itunesadvisory TEXT,
                length INTEGER,
                publisher TEXT,
                rating INTEGER,
                title TEXT,
                track_number INTEGER,
                track_total INTEGER,
                source TEXT,
                main_artist TEXT
            )""")
            self.conn.commit()
            logging.info("Created songs table")
        except:
            self.raise_table_not_created("song", "unable to create table")

    def create_plays_table(self):
        """
        create_plays_table Creates the plays table in the database.
        """
        try:
            logging.info("Creating plays table")
            cursor = self.conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS plays (
                    play_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    song_title TEXT NOT NULL,
                    song_primary_artist TEXT NOT NULL,
                    filesize BIGINT,
                    start_dt TEXT NOT NULL,
                    end_dt TEXT NOT NULL,
                    song_id TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created plays table")
        except:
            self.raise_table_not_created("plays", "unable to create table")

    def create_playlists_table(self) -> bool:
        """
        create_playlists_table Creates the playlists table in the database.

        Returns:
            bool: True if successful, False if not.
        """
        try:
            logging.info("Creating playlists table")
            cursor = self.conn.cursor()

            # description is a text that is
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS playlists (
                    playlist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    playlist_name TEXT NOT NULL,
                    playlist_desc TEXT,
                    created_dt TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created playlists table")
        except:
            self.raise_table_not_created("playlists", "unable to create table")

    def create_playlists_songs_table(self) -> bool:
        """
        create_playlists_songs_table Creates the playlists_songs table in the database.

        Returns:
            bool: True if successful, False if not.
        """
        try:
            # Song_id is a foreign key to the songs table
            logging.info("Creating playlists_songs table")
            cursor = self.conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS playlists_songs (
                    playlist_id INTEGER NOT NULL,
                    song_id TEXT NOT NULL,
                    added_dt TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created playlists_songs table")
        except:
            self.raise_table_not_created(
                "playlists_songs", "unable to create table")

    def create_song_artists_table(self):
        """
        create_song_artists_table Creates the song_artists table in the database.
        """
        try:
            # Song_id is a foreign key to the songs table
            logging.info("Creating song_artists table")
            cursor = self.conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS song_artists (
                    artist_name TEXT NOT NULL,
                    song_id TEXT NOT NULL,
                    dt_added TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created song_artists table")
        except:
            self.raise_table_not_created(
                "song_artists", "unable to create table")

    def create_album_artists_table(self):
        """Create the album_artists table, returns True if successful, False if not."""
        try:
            # Album_id is a foreign key to the songs table
            logging.info("Creating album_artists table")
            cursor = self.conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS album_artists (
                    artist_name TEXT NOT NULL,
                    song_id TEXT NOT NULL,
                    dt_added TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created album_artists table")
        except:
            self.raise_table_not_created(
                "album_artists", "unable to create table")

    def create_composers_table(self) -> bool:
        """
        create_composers_table Creates the composers table in the database.

        Returns:
            bool: True if successful, False if not.
        """
        try:
            # Song_id is a foreign key to the songs table
            logging.info("Creating composers table")
            cursor = self.conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS composers (
                    composer_name TEXT NOT NULL,
                    song_id TEXT NOT NULL,
                    dt_added TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created composers table")
        except:
            self.raise_table_not_created("composers", "unable to create table")

    def create_genres_table(self):
        """
        create_genres_table Creates the genres table in the database.
        """
        try:
            # Song_id is a foreign key to the songs table
            logging.info("Creating genres table")
            cursor = self.conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS genres (
                    genre_name TEXT NOT NULL,
                    song_id TEXT NOT NULL,
                    dt_added TEXT NOT NULL
                );"""
            )
            self.conn.commit()
            logging.info("Created genres table")
        except:
            self.raise_table_not_created("genres", "unable to create table")

    def create_all_tables(self):
        """Create all the tables, returns True if successful, False if not."""
        logging.info("Creating all tables")
        self.create_songs_table()
        self.create_plays_table()
        self.create_playlists_table()
        self.create_playlists_songs_table()
        self.create_song_artists_table()
        self.create_album_artists_table()
        self.create_composers_table()
        self.create_genres_table()
        logging.info("Created all tables")


    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    #                                          DELETE TABLES & INFO  
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    def clear_songs_table(self):
        """
        clear_songs_table Clears the songs table in the database.
        """
        logging.info("Clearing songs table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM songs;")
        self.conn.commit()
        logging.info("Cleared songs table")

    def clear_plays_table(self):
        """
        clear_plays_table Clears the plays table in the database.
        """
        logging.info("Clearing plays table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM plays;")
        self.conn.commit()
        logging.info("Cleared plays table")

    def clear_playlists_table(self):
        """
        clear_playlists_table Clears the playlists table in the database.
        """
        logging.info("Clearing playlists table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM playlists;")
        self.conn.commit()
        logging.info("Cleared playlists table")

    def clear_playlists_songs_table(self):
        """
        clear_playlists_songs_table Clears the playlists_songs table in the database.
        """
        logging.info("Clearing playlists_songs table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM playlists_songs;")
        self.conn.commit()
        logging.info("Cleared playlists_songs table")

    def clear_song_artists_table(self):
        """
        clear_song_artists_table Clears the song_artists table in the database.
        """
        logging.info("Clearing song_artists table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM song_artists;")
        self.conn.commit()
        logging.info("Cleared song_artists table")

    def clear_album_artists_table(self):
        """
        clear_album_artists_table Clears the album_artists table in the database.
        """
        logging.info("Clearing album_artists table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM album_artists;")
        self.conn.commit()
        logging.info("Cleared album_artists table")

    def clear_composers_table(self):
        """
        clear_composers_table Clears the composers table in the database.
        """
        logging.info("Clearing composers table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM composers;")
        self.conn.commit()
        logging.info("Cleared composers table")

    def clear_genres_table(self):
        """
        clear_genres_table Clears the genres table in the database.
        """
        logging.info("Clearing genres table")
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM genres;")
        self.conn.commit()
        logging.info("Cleared genres table")

        
    def clear_all_tables(self) -> bool:
        """Clear all the tables, returns True if successful, False if not."""
        logging.info("Clearing all tables")
        self.clear_songs_table()
        self.clear_plays_table()
        self.clear_playlists_table()
        self.clear_playlists_songs_table()
        self.clear_song_artists_table()
        self.clear_album_artists_table()
        self.clear_composers_table()
        self.clear_genres_table()
        logging.info("Cleared all tables")
        return True

    def delete_database(self) -> bool:
        """Delete the database, returns True if successful, False if not.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info("Deleting database")
        os.remove(config.DATABASE_PATH)
        logging.info("Deleted database")
        return True

    def delete_song_by_id(self, song_id: str) -> bool:
        """Delete a song from the database by its ID.

        Args:
            song_id (str): ID of the song to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting song by ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM songs WHERE song_id = ?;""",
            (song_id,)
        )
        self.conn.commit()
        logging.info(f"Deleted song by ID: {song_id}")
        return True

    def delete_playlist_by_id(self, playlist_id: int) -> bool:
        """Delete a playlist from the database by its ID.

        Args:
            playlist_id (int): ID of the playlist to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting playlist by ID: {playlist_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM playlists WHERE playlist_id = ?;""",
            (playlist_id,)
        )
        self.conn.commit()
        logging.info(f"Deleted playlist by ID: {playlist_id}")
        return True

    def delete_playlist_by_name(self, playlist_name: str) -> bool:
        """Delete a playlist from the database by its name.

        Args:
            playlist_name (str): Name of the playlist to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting playlist by name: {playlist_name}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM playlists WHERE playlist_name = ?;""",
            (playlist_name,)
        )
        self.conn.commit()
        logging.info(f"Deleted playlist by name: {playlist_name}")
        return True

    def delete_playlist_song_by_name(self, playlist_name: str, song_name: str) -> bool:
        """Delete a song from a playlist by its name.

        Args:
            playlist_name (str): Name of the playlist to delete the song from.
            song_name (str): Name of the song to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting song from playlist by name: {song_name}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM playlists_songs WHERE playlist_id = (SELECT playlist_id FROM playlists WHERE playlist_name = ?) AND song_id = (SELECT song_id FROM songs WHERE song_name = ?);""",
            (playlist_name, song_name)
        )
        self.conn.commit()
        logging.info(f"Deleted song from playlist by name: {song_name}")
        return True

    def delete_playlist_song_by_playlist_id_song_id(self, playlist_id: int, song_id: str) -> bool:
        """Delete a song from a playlist by its ID.

        Args:
            playlist_id (int): ID of the playlist to delete the song from.
            song_id (str): ID of the song to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting song from playlist by ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM playlists_songs WHERE playlist_id = ? AND song_id = ?;""",
            (playlist_id, song_id)
        )
        self.conn.commit()
        logging.info(f"Deleted song from playlist by ID: {song_id}")
        return True

    def delete_song_artist_by_artist_song_id(self, artist_id: int, song_id: str) -> bool:
        """Delete a song from a playlist by its ID.

        Args:
            artist_id (int): ID of the artist to delete the song from.
            song_id (str): ID of the song to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting song from artist by ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM songs_artists WHERE artist_id = ? AND song_id = ?;""",
            (artist_id, song_id)
        )
        self.conn.commit()
        logging.info(f"Deleted song from artist by ID: {song_id}")
        return True

    def delete_album_artist_by_artist_album_id(self, artist_id: int, album_id: int) -> bool:
        """Delete an album from an artist by its ID.

        Args:
            artist_id (int): ID of the artist to delete the album from.
            album_id (int): ID of the album to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting album from artist by ID: {album_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM albums_artists WHERE artist_id = ? AND album_id = ?;""",
            (artist_id, album_id)
        )
        self.conn.commit()
        logging.info(f"Deleted album from artist by ID: {album_id}")
        return True

    def delete_composer_by_name_song_id(self, composer_name: str, song_id: str) -> bool:
        """Delete a composer from a song by its name.

        Args:
            composer_name (str): Name of the composer to delete the song from.
            song_id (str): ID of the song to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting composer from song by name: {composer_name}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM songs_composers WHERE composer_id = (SELECT composer_id FROM composers WHERE composer_name = ?) AND song_id = ?;""",
            (composer_name, song_id)
        )
        self.conn.commit()
        logging.info(f"Deleted composer from song by name: {composer_name}")
        return True

    def delete_genre_by_name_song_id(self, genre_name: str, song_id: str) -> bool:
        """Delete a genre from a song by its name.

        Args:
            genre_name (str): Name of the genre to delete the song from.
            song_id (str): ID of the song to delete.

        Returns:
            bool: True if successful, False if not.
        """
        logging.info(f"Deleting genre from song by name: {genre_name}")
        cursor = self.conn.cursor()
        cursor.execute(
            """DELETE FROM songs_genres WHERE genre_id = (SELECT genre_id FROM genres WHERE genre_name = ?) AND song_id = ?;""",
            (genre_name, song_id)
        )
        self.conn.commit()
        logging.info(f"Deleted genre from song by name: {genre_name}")
        return True
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    #                                    RETRIEVE DATA INDIVIDUAL
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    def get_song_by_id(self, song_id: str) -> List[dict]:
        """
        get_song_by_id Searches the song table for a song with the given ID.

        Args:
            song_id (str): ID of the song to search for.

        Returns:
            List[dict]: List of dictionaries containing the song data.
        """

        logging.info(f"Getting song by ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM songs WHERE song_id = ?;""",
            (song_id,)
        )
        song = cursor.fetchone()

        # copy the dict variable config.song_table_data
        song_table_data = config.song_table_data.copy()
        
        if song is None:
            return None
        song_table_data['song_id'] = song[0]
        song_table_data["filepath"] = song[1]
        song_table_data["filesize"] = song[2]
        song_table_data["padding"] = song[3]
        song_table_data["album_artwork_bit_depth"] = song[4]
        song_table_data["album_artwork_colors"] = song[5]
        song_table_data["album_artwork_height"] = song[6]
        song_table_data["album_artwork_width"] = song[7]
        song_table_data["bit_depth"] = song[8]
        song_table_data["bitrate"] = song[9]
        song_table_data["channels"] = song[10]
        song_table_data["duration"] = song[11]
        song_table_data["sample_rate"] = song[12]
        song_table_data["album"] = song[13]
        song_table_data["barcode"] = song[14]
        song_table_data["date_created"] = song[15]
        song_table_data["disc_number"] = song[16]
        song_table_data["disc_total"] = song[17]
        song_table_data["isrc"] = song[18]
        song_table_data["itunesadvisory"] = song[19]
        song_table_data["length"] = song[20]
        song_table_data["publisher"] = song[21]
        song_table_data["rating"] = song[22]
        song_table_data["title"] = song[23]
        song_table_data["track_number"] = song[24]
        song_table_data["track_total"] = song[25]
        song_table_data["source"] = song[26]
        song_table_data["main_artist"] = song[27]
        return song_table_data

    def get_song_id_by_title_filesize(self, title: str, filesize: int) -> int:
        """
        get_song_id_by_title_filesize Searches the database to find the song ID by title and filesize. 
        You want to use filesize because they're basically gurenteed to be unique.

        Args:
            title (str): Title of Song
            filesize (int): Filesize of song (get these from the song object)

        Returns:
            int: song_id of the song
        """

        logging.info(
            f"Getting song by title and filesize: {title}, {filesize}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM songs WHERE title = ? AND filesize = ?;""",
            (title, filesize)
        )
        # return the id of the song
        song = cursor.fetchone()
        if song is None:
            return None
        logging.info(f"Got song by title and filesize: {title}, {filesize}")
        return song[0]

    def get_songs_in_playlist(self, playlist_name: str) -> List[dict]:
        """
        get_songs_in_playlist Returns a list of all the songs in a given playlist

        Args:
            playlist_name (str): Name of the playlist to get songs from

        Returns:
            List[dict]: List of dictionaries containing the song data.
        """

        logging.info(f"Getting songs in playlist: {playlist_name}")

        playlist_id = self.get_playlist_id_by_name(playlist_name)

        # GET SONG IDS from playlists_songs table then look up songs in songs table
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM playlists_songs WHERE playlist_id = ?;""",
            (playlist_id,)
        )
        playlist_songs = cursor.fetchall()
        songs = []
        for playlist_song in playlist_songs:
            song_id = playlist_song[1]
            song = self.get_song_by_id(song_id)
            songs.append(song)
        logging.info(f"Got songs in playlist: {playlist_name}")

        return songs

    def get_playlist_id_by_name(self, playlist_name: str) -> int:
        """
        get_playlist_id_by_name Get ID of playlist by name.

        Args:
            playlist_name (str): Name of playlist

        Returns:
            int: ID of playlist
        """

        logging.info(f"Getting playlist by name: {playlist_name}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM playlists WHERE playlist_name = ?;""",
            (playlist_name,)
        )
        playlist = cursor.fetchone()
        if playlist is None:
            return None
        logging.info(f"Got playlist by name: {playlist_name}")
        return playlist[0]

    def get_playlist_by_id(self, playlist_id: int) -> dict:
        """
        get_playlist_by_id Get playlist by ID.

        Args:
            playlist_id (int): ID of playlist

        Returns:
            dict: Dictionary containing playlist data. {playlist_id, playlist_name, playlist_desc, created_dt}
        """

        logging.info(f"Getting playlist by ID: {playlist_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM playlists WHERE playlist_id = ?;""",
            (playlist_id,)
        )
        playlist = cursor.fetchone()
        logging.info(f"Got playlist by ID: {playlist_id}")

        playlist_data = {}
        playlist_data["playlist_id"] = playlist[0]
        playlist_data["playlist_name"] = playlist[1]
        playlist_data["playlist_desc"] = playlist[2]
        playlist_data["created_dt"] = playlist[3]
        return playlist_data

    def get_song_album_artists(self, song_id: str) -> List[str]:
        """
        get_song_album_artists Get all the album artists of a song, returns a list of names.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of album artists
        """

        logging.info(f"Getting song album artists by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM album_artists WHERE song_id = ?;""",
            (song_id,)
        )
        album_artists = cursor.fetchall()
        logging.info(f"Got song album artists by song ID: {song_id}")
        return album_artists

    def get_song_composers(self, song_id: str) -> List[str]:
        """
        get_song_composers Get all the composers of a song, returns a list of names.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of composers
        """

        logging.info(f"Getting song composers by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM composers WHERE song_id = ?;""",
            (song_id,)
        )
        composers = cursor.fetchall()
        logging.info(f"Got song composers by song ID: {song_id}")
        return composers

    def get_song_artists(self, song_id: str) -> List[str]:
        """
        get_song_artists Get all the artists of a song, returns a list of names of Artists.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of artists
        """

        logging.info(f"Getting song artists by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM artists WHERE song_id = ?;""",
            (song_id,)
        )
        artists = cursor.fetchall()
        logging.info(f"Got song artists by song ID: {song_id}")
        return artists

    def get_song_genres(self, song_id: str) -> List[str]:
        """
        get_song_genres Get all the genres of a song, returns a list of names of genres.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of genres
        """

        logging.info(f"Getting song genres by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM genres WHERE song_id = ?;""",
            (song_id,)
        )
        genres = cursor.fetchall()
        logging.info(f"Got song genres by song ID: {song_id}")
        return genres

    def get_play_information_from_song_id(self, song_id: str) -> Dict[str, str]:
        """
        get_play_information_from_song_id Get play information from song ID. This is the song_title, song_primary_artist, and filesize

        Args:
            song_id (str): ID of song

        Returns:
            Dict[str, str]: Dictionary of song_title, song_primary_artist, filesize, and song_id
        """        

        logging.info(f"Getting play information for song ID: {song_id}")

        song_raw = self.get_song_by_id(song_id)
        song_values = {
            "song_title": song_raw["title"],
            "song_primary_artist": song_raw["main_artist"],
            "filesize": song_raw["filesize"],
            "song_id": song_raw['song_id']
        }

        logging.info(f"Got play information for song ID: {song_id}")
        return song_values

    def get_play_by_id(self, play_id: int) -> dict:
        """
        get_play_by_id Get play by ID.

        Args:
            play_id (int): ID of play

        Returns:
            dict: play object
        """

        logging.info(f"Getting play by ID: {play_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM plays WHERE play_id = ?;""",
            (play_id,)
        )
        play = cursor.fetchone()
        logging.info(f"Got play by ID: {play_id}")

        play_data = {
            "play_id": play[0],
            "song_title": play[1],
            "song_primary_artist": play[2],
            "filesize": play[3],
            "start_dt": play[4],
            "end_dt": play[5],
            "song_id": play[6]
        }
        return play_data

    def get_song_artists_of_song(self, song_id: str) -> List[str]:
        """
        get_song_artists_of_song Get all the artists of a song, returns a list of names of Artists.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of artists
        """

        logging.info(f"Getting song artists by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM song_artists WHERE song_id = ?;""",
            (song_id,)
        )
        artists = cursor.fetchall()
        song_artists = []
        for artist in artists:
            song_artists.append(artist[0])
        logging.info(f"Got song artists by song ID: {song_id}")
        return song_artists
  
    def get_album_artists_of_song(self, song_id: str) -> List[str]:
        """
        get_album_artists_of_song Get all the artists of a song, returns a list of names of Artists.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of artists
        """

        logging.info(f"Getting album artists by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM album_artists WHERE song_id = ?;""",
            (song_id,)
        )
        artists = cursor.fetchall()
        album_artists = []
        for artist in artists:
            album_artists.append(artist[0])
        logging.info(f"Got album artists by song ID: {song_id}")
        return album_artists
  
    def get_composers_of_song(self, song_id: str) -> List[str]:
        """
        get_composers_of_song Get all the composers of a song, returns a list of names of composers.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of composers
        """

        logging.info(f"Getting composers by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM composers WHERE song_id = ?;""",
            (song_id,)
        )
        composers = cursor.fetchall()
        song_composers = []
        for composer in composers:
            song_composers.append(composer[0])
        logging.info(f"Got composers by song ID: {song_id}")
        return song_composers

    def get_genres_of_song(self, song_id: str) -> List[str]:
        """
        get_genres_of_song Get all the genres of a song, returns a list of names of genres.

        Args:
            song_id (str): ID of song

        Returns:
            List[str]: list of names of genres
        """

        logging.info(f"Getting song genres by song ID: {song_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM genres WHERE song_id = ?;""",
            (song_id,)
        )
        genres = cursor.fetchall()
        song_genres = []
        for genre in genres:
            song_genres.append(genre[0])
        logging.info(f"Got song genres by song ID: {song_id}")
        return song_genres
 
    def get_songs_in_album(self, album_name: str, album_artist: str) -> List[dict]:
        """
        get_songs_in_album Get all the songs in an album, returns a list of song objects.

        Args:
            album_name (str): name of album
            album_artist (str): name of album artist

        Returns:
            List[dict]: list of song objects (song_title, song_primary_artist, filesize, song_id)
        """

        logging.info(f"Getting songs in album: {album_name}")
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM songs WHERE album = ? AND main_artist = ?;""",
            (album_name, album_artist)
        )
        songs = cursor.fetchall()
        song_list = []
        for song in songs:
            song_list.append({
                "song_title": song[-5],
                "song_primary_artist": song[-1],
                "filesize": song[2],
                "song_id": song[0]
            })
        logging.info(f"Got songs in album: {album_name}")
        return song_list
 
    def get_songs_in_album_artist(self, album_artist: str) -> List[dict]:
        """
        get_songs_in_album_artist Get all the songs in an album artist, returns a list of Song objects.

        Args:
            album_artist (str): name of album artist

        Returns:
            List[dict]: list of Song objects (song_title, song_primary_artist, filesize, song_id)
        """

        logging.info(f"Getting songs in album artist: {album_artist}")
        cursor = self.conn.cursor()
        # use table album_artists to get all songs by album artist
        cursor.execute(
            """SELECT * FROM album_artists WHERE artist_name = ?;""",
            (album_artist,)
        )
        
        songs = cursor.fetchall()
        song_list = []
        for song in songs:
            song_data = self.get_song_by_id(song[1])
            song_list.append({
                "song_title": song_data['title'],
                "song_primary_artist": song_data['main_artist'],
                "filesize": song_data['filesize'],
                "song_id": song_data['song_id']
            })
        logging.info(f"Got songs in album artist: {album_artist}")

        return song_list
  
    def get_songs_in_song_artist(self, song_artist: str) -> List[dict]:
        """
        get_songs_in_song_artist Get all the songs in a song artist, returns a list of Song objects.

        Args:
            song_artist (str): name of song artist

        Returns:
            List[dict]: list of Song objects (song_title, song_primary_artist, filesize, song_id)
        """

        logging.info(f"Getting songs in song artist: {song_artist}")
        cursor = self.conn.cursor()
        # use table song_artists to get all songs by song artist
        cursor.execute(
            """SELECT * FROM song_artists WHERE artist_name = ?;""",
            (song_artist,)
        )
        
        songs = cursor.fetchall()
        song_list = []
        for song in songs:
            song_data = self.get_song_by_id(song[1])
            song_list.append({
                "song_title": song_data['title'],
                "song_primary_artist": song_data['main_artist'],
                "filesize": song_data['filesize'],
                "song_id": song_data['song_id']
            })
        logging.info(f"Got songs in song artist: {song_artist}")

        return song_list
  
    def get_songs_in_composer(self, composer: str) -> List[dict]:
        """
        get_songs_in_composer Get all the songs in a composer, returns a list of Song objects.

        Args:
            composer (str): name of composer

        Returns:
            List[dict]: list of Song objects (song_title, song_primary_artist, filesize, song_id)
        """

        logging.info(f"Getting songs in composer: {composer}")
        cursor = self.conn.cursor()
        # use table composers to get all songs by composer
        cursor.execute(
            """SELECT * FROM composers WHERE composer_name = ?;""",
            (composer,)
        )
        
        songs = cursor.fetchall()
        song_list = []
        for song in songs:
            song_data = self.get_song_by_id(song[1])
            song_list.append({
                "song_title": song_data['title'],
                "song_primary_artist": song_data['main_artist'],
                "filesize": song_data['filesize'],
                "song_id": song_data['song_id']
            })
        logging.info(f"Got songs in composer: {composer}")

        return song_list

    def get_songs_in_genre(self, genre: str) -> List[dict]:
        """
        get_songs_in_genre Get all the songs in a genre, returns a list of Song objects.

        Args:
            genre (str): name of genre

        Returns:
            List[dict]: list of Song objects (song_title, song_primary_artist, filesize, song_id)
        """

        logging.info(f"Getting songs in genre: {genre}")
        cursor = self.conn.cursor()
        # use table genres to get all songs by genre
        cursor.execute(
            """SELECT * FROM genres WHERE genre_name = ?;""",
            (genre,)
        )
        
        songs = cursor.fetchall()
        song_list = []
        for song in songs:
            song_data = self.get_song_by_id(song[1])
            song_list.append({
                "song_title": song_data['title'],
                "song_primary_artist": song_data['main_artist'],
                "filesize": song_data['filesize'],
                "song_id": song_data['song_id']
            })
        logging.info(f"Got songs in genre: {genre}")

        return song_list
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    #                                    RETRIEVE DATA MULTIPLE
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    def get_all_tables(self) -> List[str]:
        """
        get_all_tables Get all the tables in the database, returns a list of table names.

        Returns:
            List[str]: list of table names
        """

        logging.info("Getting all tables")
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logging.info("Got all tables")
        return tables

    def get_all_songs(self) -> List[dict]:
        """
        get_all_songs Get all the songs in the database, returns a list of Song objects

        Returns:
            List[dict]: list of dictionaries
        """

        logging.info("Getting all songs")
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM songs;")
        songs = cursor.fetchall()
        songs_return = []
        for song in songs:
            song_table_data = {
                "song_id": song[0],
                "filepath": song[1],
                "filesize": song[2],
                "padding": song[3],
                "album_artwork_bit_depth": song[4],
                "album_artwork_colors": song[5],
                "album_artwork_height": song[6],
                "album_artwork_width": song[7],
                "bit_depth": song[8],
                "bitrate": song[9],
                "channels": song[10],
                "duration": song[11],
                "sample_rate": song[12],
                "album": song[13],
                "barcode": song[14],
                "date_created": song[15],
                "disc_number": song[16],
                "disc_total": song[17],
                "isrc": song[18],
                "itunesadvisory": song[19],
                "length": song[20],
                "publisher": song[21],
                "rating": song[22],
                "title": song[23],
                "track_number": song[24],
                "track_total": song[25],
                "source": song[26],
                "main_artist": song[27],
            }
            songs_return.append(song_table_data)
        return songs_return

    def get_all_plays(self) -> List[dict]:
        """
        get_all_plays Get all the plays in the database, returns a list of the plays

        Returns:
            List[dict]: list of dictionaries
        """

        logging.info("Getting all plays")
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM plays;")
        plays = cursor.fetchall()
        logging.info("Got all plays")
        return plays

    def get_all_song_artists(self, no_duplicates=True) -> List[str]:
        """
        get_all_song_artists Get all the song artists in the database, returns a list of strings

        Returns:
            List[str]: list of strings
        """

        # artists can be duplicated, so we need to remove duplicates from song_artists
        logging.info("Getting all song artists")
        cursor = self.conn.cursor()
        if no_duplicates:
            cursor.execute("SELECT DISTINCT artist_name FROM song_artists;")
        else:
            cursor.execute("SELECT artist_name FROM song_artists;")
        song_artists = cursor.fetchall()
        logging.info("Got all song artists")
        song_artists = [artist[0] for artist in song_artists]
        return song_artists

    def get_all_album_artists(self, no_duplicates=True) -> List[str]:
        """
        get_all_album_artists Get all the album artists in the database, returns a list of strings

        Args:
            no_duplicates (bool, optional): remove duplicates. Defaults to True.

        Returns:
            List[str]: list of strings
        """

        # artists can be duplicated, so we need to remove duplicates from album_artists
        logging.info("Getting all album artists")
        cursor = self.conn.cursor()
        album_artists = []
        if no_duplicates:
            cursor.execute("SELECT DISTINCT artist_name FROM album_artists;")
        else:
            cursor.execute("SELECT artist_name FROM album_artists;")
        album_artists = cursor.fetchall()
        logging.info("Got all album artists")
        album_artists = [artist[0] for artist in album_artists]
        return album_artists

    def get_all_composers(self, no_duplicates=True) -> List[str]:
        """
        get_all_composers Get all the composers in the database, returns a list of strings

        Args:
            no_duplicates (bool, optional): remove duplicates. Defaults to True.

        Returns:
            List[str]: list of strings
        """

        # composers can be duplicated, so we need to remove duplicates from composers
        logging.info("Getting all composers")
        cursor = self.conn.cursor()
        if no_duplicates:
            cursor.execute("SELECT DISTINCT composer_name FROM composers;")
        else:
            cursor.execute("SELECT composer_name FROM composers;")
        composers = cursor.fetchall()
        logging.info("Got all composers")
        composers = [composer[0] for composer in composers]
        return composers

    def get_all_genres(self, no_duplicates=True) -> List[str]:
        """
        get_all_genres Get all the genres in the database, returns a list of strings

        Args:
            no_duplicates (bool, optional): remove duplicates. Defaults to True.

        Returns:
            List[str]: list of strings
        """

        # genres can be duplicated, so we need to remove duplicates from genres
        logging.info("Getting all genres")
        cursor = self.conn.cursor()
        if no_duplicates:
            cursor.execute("SELECT DISTINCT genre_name FROM genres;")
        else:
            cursor.execute("SELECT genre_name FROM genres;")
        genres = cursor.fetchall()
        logging.info("Got all genres")
        genres = [genre[0] for genre in genres]
        return genres

    def get_all_playlist_names(self) -> List[str]:
        """
        get_all_playlist_names Get all the playlist names in the database, returns a list of strings

        Returns:
            List[str]: list of strings
        """

        logging.info("Getting all playlist names")
        cursor = self.conn.cursor()
        cursor.execute("SELECT playlist_name FROM playlists;")
        playlist_names = cursor.fetchall()

        # remove the tuple from each playlist name
        playlist_names = [playlist_name[0] for playlist_name in playlist_names]
        logging.info("Got all playlist names")
        return playlist_names

    def get_all_playlist_songs(self) -> List[dict]:
        """
        get_all_playlist_songs Get all the playlist songs in the database, returns a list of dictionaries

        Returns:
            List[dict]: list of dictionaries [song_name, file_size, id]
        """

        playlist_names = self.get_all_playlist_names()
        playlist_songs = {}
        for playlist_name in playlist_names:
            # we only want the song_name, file_size, and id
            songs = self.get_songs_in_playlist(playlist_name)
            playlist_values = []
            for song_data in songs:
                song_values = {
                    "song_name": song_data["title"],
                    "file_size": song_data["filesize"],
                }

                song_id = self.get_song_id_by_title_filesize(
                    song_values["song_name"], song_values["file_size"])
                song_values["id"] = song_id
                playlist_values.append(song_values)

            playlist_songs[playlist_name] = playlist_values
        return playlist_songs

    def get_all_songs_in_genre(self, genre_name: str) -> List[dict]:
        """
        get_all_songs_in_genre Get all the songs in a genre, returns a list of dictionaries

        Args:
            genre_name (str): Name of the genre

        Returns:
            List[dict]: list of dictionaries
        """
            
        logging.info("Getting all songs in genre {}".format(genre_name))
        cursor = self.conn.cursor()

        # get all song_id that matches the given genre_name in the genres table
        
        cursor.execute(
            "SELECT song_id FROM genres WHERE genre_name = ?;", (genre_name,))
        song_ids = cursor.fetchall()
        song_ids = [song_id[0] for song_id in song_ids]

        # get all the songs that match the song_ids
        songs = []
        for song_id in song_ids:
            song_data = self.get_song_by_id(song_id)
            songs.append(song_data)

        logging.info("Got all songs in genre {}".format(genre_name))
        return songs

    def get_all_columns_from_table(self, table_name: str) -> List[str]:
        """
        get_all_columns_from_table Get all the columns from a table, returns a list of strings

        Args:
            table_name (str): Name of the table

        Returns:
            List[str]: list of strings
        """

        logging.info("Getting all columns from table {}".format(table_name))
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info({});".format(table_name))
        columns = cursor.fetchall()
        columns = [column[1] for column in columns]
        logging.info("Got all columns from table {}".format(table_name))
        return columns

    def get_all_albums(self) -> List[Dict[str, str]]:
        """
        get_all_albums Get all the albums in the database, returns a list of dictionaries

        Returns:
            List[Dict[str, str]]: list of dictionaries, artist_name, album_name
        """

        # need to get all albums from song table and pair it with the artist_name from the artists table
        logging.info("Getting all albums")
        cursor = self.conn.cursor()
        cursor.execute("SELECT album, main_artist FROM songs;")
        albums = cursor.fetchall()
        albums = [{"artist_name": album[1], "album_name": album[0]} for album in albums]

        # now we need to remove duplicates
        albums = [dict(t) for t in {tuple(d.items()) for d in albums}]
        logging.info("Got all albums")
        return albums
  
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    #                                    INSERT DATA
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    # play table: song_title, song_primary_artist, filesize, start_dt, end_dt
    def insert_play(self, song_title: str, song_primary_artist: str, filesize: int, start_dt: str, end_dt: str, song_id: str) -> int:
        """
        insert_play Insert a play into the plays table

        Args:
            song_title (str): Title of the song
            song_primary_artist (str): Primary artist of the song
            filesize (int): Filesize of the song
            start_dt (str): Date the song started playing
            end_dt (str): Date the song ended playing

        Returns:
            int: id of the inserted play
        """


        # plays table has auto incrementing id, so we don't need to insert the id
        logging.info("Inserting play into plays table")
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO plays (song_title, song_primary_artist, filesize, start_dt, end_dt, song_id) VALUES (?, ?, ?, ?, ?, ?);",
            (song_title, song_primary_artist, filesize, start_dt, end_dt, song_id)
        )
        self.conn.commit()
        
        # get the id of the inserted play
        play_id = cursor.lastrowid
        logging.info("Inserted play into plays table")
        return play_id

    def insert_playlist(self, playlist_name: str, playlist_desc: str, created_dt: str) -> int:
        """
        insert_playlist Insert a playlist into the playlists table. Only inserts if the playlist does not already exist

        Args:
            playlist_name (str): Name of the playlist
            playlist_desc (str): Description of the playlist
            created_dt (str): Date the playlist was created

        Returns:
            int: id of the inserted playlist
        """
        
        # check if the playlist already exists
        if self.get_playlist_id_by_name(playlist_name) is not None:
            logging.info("Playlist {} already exists".format(playlist_name))
            return False

        logging.info("Inserting playlist {} into playlists table".format(playlist_name))
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO playlists (playlist_name, playlist_desc, created_dt) VALUES (?, ?, ?);", (playlist_name, playlist_desc, created_dt))
        self.conn.commit()

        # get the id of the inserted playlist
        playlist_id = cursor.lastrowid
        logging.info("Inserted playlist {} into playlists table".format(playlist_name))
        return playlist_id


    def insert_playlist_song(self, playlist_name: str, song_id: str) -> bool:
        """
        insert_playlist_song Inserts a song into a playlist. Adds a record to the playlists_songs table

        Args:
            playlist_name (str): Name of the playlist
            song_id (str): ID of the song

        Returns:
            bool: True if successful, False if not
        """
        logging.info(
            "Inserting playlist_song {} into playlists_songs table".format(playlist_name))

        playlist_id = self.get_playlist_id_by_name(playlist_name)
        added_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor = self.conn.cursor()
        # duplicates are ok

        cursor.execute(
            """INSERT INTO playlists_songs (playlist_id, song_id, added_dt)
            VALUES (?, ?, ?);""",
            (playlist_id, song_id, added_dt)
        )
        self.conn.commit()
        logging.info(
            "Inserted playlist_song {} into playlists_songs table".format(playlist_name))
        return True

    def insert_song(self, **kwargs) -> str:
        """
        insert_song Insert a song into the songs table, returns song_id of inserted song.
        Only insert if the song does not already exist. Use the title and filesize.

        Args:
            **kwargs: song_table_data (dict)

        Returns:
            str: song_id of inserted song
        """
        logging.info(
            "Inserting song {} into songs table".format(kwargs["title"]))
        cursor = self.conn.cursor()
        # self.song_table_data = {
        #     "song_id": "N/A", # string
        #     "filepath": "N/A", # string
        #     "main_artist": "N/A", # string
        #     "filesize": -1, # in bytes
        #     "padding": -1, # in bytes
        #     "album_artwork_bit_depth": -1, # in bits
        #     "album_artwork_colors": -1, # int
        #     "album_artwork_height": -1, # in pixels
        #     "album_artwork_width": -1, # in pixels
        #     "bit_depth": -1, # in bits
        #     "bitrate": -1, # in bits, divide by 1000 to get Kbps
        #     "channels": -1, # int
        #     "duration": -1, # in seconds
        #     "sample_rate": -1, # in KHz
        #     "album": "N/A", # string
        #     "barcode": "N/A", # string
        #     "date_created": "N/A", # in YYYY-MM-DD
        #     "disc_number": -1, # int
        #     "disc_total": -1, # int
        #     "isrc": "N/A", # string
        #     "itunesadvisory": "N/A", # string
        #     "length": -1, # int
        #     "publisher": "N/A", # string
        #     "rating": -1, # int
        #     "title": "N/A", # string
        #     "track_number": -1, # int
        #     "track_total": -1, # int
        #     "source": "N/A", # string
        # }

        # check if song already exists
        song_id = self.get_song_by_id(kwargs['song_id'])
        if song_id:
            logging.warning("Song {} already exists in songs table".format(
                kwargs["title"]))
            return song_id
        cursor.execute(
            """INSERT INTO songs (
                song_id,
                filepath,
                main_artist,
                filesize,
                padding,
                album_artwork_bit_depth,
                album_artwork_colors,
                album_artwork_height,
                album_artwork_width,
                bit_depth,
                bitrate,
                channels,
                duration,
                sample_rate,
                album,
                barcode,
                date_created,
                disc_number,
                disc_total,
                isrc,
                itunesadvisory,
                length,
                publisher,
                rating,
                title,
                track_number,
                track_total,
                source
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );""",
            (
                kwargs['song_id'],
                kwargs["filepath"],
                kwargs["main_artist"],
                kwargs["filesize"],
                kwargs["padding"],
                kwargs["album_artwork_bit_depth"],
                kwargs["album_artwork_colors"],
                kwargs["album_artwork_height"],
                kwargs["album_artwork_width"],
                kwargs["bit_depth"],
                kwargs["bitrate"],
                kwargs["channels"],
                kwargs["duration"],
                kwargs["sample_rate"],
                kwargs["album"],
                kwargs["barcode"],
                kwargs["date_created"],
                kwargs["disc_number"],
                kwargs["disc_total"],
                kwargs["isrc"],
                kwargs["itunesadvisory"],
                kwargs["length"],
                kwargs["publisher"],
                kwargs["rating"],
                kwargs["title"],
                kwargs["track_number"],
                kwargs["track_total"],
                kwargs["source"],
            )
        )

        self.conn.commit()
        # get the song_id of the inserted song
    
        song_name = kwargs["title"]
        logging.info(f"Inserted {song_name} with song_id: {song_id}")
        return kwargs['song_id']

    def insert_album_artist(self, artist_name, song_id) -> bool:
        """
        insert_album_artist Insert an album_artist into the database, returns True if successful, False if not. Only insert if the album_artist does not already exist

        Args:
            artist_name (_type_): name of the album artist
            song_id (_type_): song_id of the song

        Returns:
            bool: True if successful, False if not
        """
        logging.info("Attempting to insert album artist {} iwith song_id {} into album_artists table".format(
            artist_name, song_id))
        cursor = self.conn.cursor()
        # check if album_artist already exists
        cursor.execute(
            """SELECT 1 FROM album_artists WHERE artist_name = ? AND song_id = ?;""",
            (artist_name, song_id)
        )

        if cursor.fetchone():
            logging.warning("Album artist {} with song_id {} already exists in album_artists table".format(
                artist_name, song_id))
            return False

        cursor.execute(
            """INSERT INTO album_artists (artist_name, song_id, dt_added)
            VALUES (?, ?, ?);""",
            (artist_name, song_id, datetime.datetime.now())
        )

        self.conn.commit()
        logging.info("Inserted album artist {} with song_id {} into album_artists table".format(
            artist_name, song_id))

        return True

    def insert_song_artist(self, artist_name, song_id) -> bool:
        """
        insert_song_artist Insert a song_artist into the database, returns True if successful, False if not. Only insert if the song_artist does not already exist

        Args:
            artist_name (_type_): name of the song artist
            song_id (_type_): song_id of the song

        Returns:
            bool: True if successful, False if not
        """

        logging.info("Attempting to insert song artist {} with song_id {} into song_artists table".format(
            artist_name, song_id))
        cursor = self.conn.cursor()
        # cursor.execute(
        #     """INSERT INTO song_artists (artist_name, song_id, dt_added)
        #     SELECT ?, ?, ? WHERE NOT EXISTS (
        #         SELECT 1 FROM song_artists WHERE artist_name = ? AND song_id = ?
        #     );""",
        #     (artist_name, song_id, datetime.datetime.now(), artist_name, song_id)
        # )

        # check if song_artist already exists
        cursor.execute(
            """SELECT 1 FROM song_artists WHERE artist_name = ? AND song_id = ?;""",
            (artist_name, song_id)
        )

        if cursor.fetchone():
            logging.warning("Song artist {} with song_id {} already exists in song_artists table".format(
                artist_name, song_id))
            return False

        cursor.execute(
            """INSERT INTO song_artists (artist_name, song_id, dt_added)
            VALUES (?, ?, ?);""",
            (artist_name, song_id, datetime.datetime.now())
        )

        self.conn.commit()
        logging.info("Inserted song artist {} with song_id {} into song_artists table".format(
            artist_name, song_id))
        return True

    def insert_composer(self, composer_name, song_id) -> bool:
        """
        insert_composer Insert a composer into the database, returns True if successful, False if not. 
        Only insert if the composer does not already exist

        Args:
            composer_name (_type_): name of the composer
            song_id (_type_): song_id of the song

        Returns:
            bool: True if successful, False if not
        """
        logging.info("Attempting to insert composer {} with song_id {} into composers table".format(
            composer_name, song_id))
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT 1 FROM composers WHERE composer_name = ? AND song_id = ?;""",
            (composer_name, song_id)
        )

        if cursor.fetchone():
            logging.warning("Composer {} with song_id {} already exists in composers table".format(
                composer_name, song_id))
            return False

        cursor.execute(
            """INSERT INTO composers (composer_name, song_id, dt_added)
            VALUES (?, ?, ?);""",
            (composer_name, song_id, datetime.datetime.now())
        )

        self.conn.commit()
        logging.info("Inserted composer {} with song_id {} into composers table".format(
            composer_name, song_id))
        return True

    def insert_genre(self, genre_name, song_id) -> bool:
        """Insert a genre into the database, returns True if successful, False if not. Only insert if the genre does not already exist."""
        logging.warning("Inserting genre {} with song_id {} into genres table".format(
            genre_name, song_id))
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT 1 FROM genres WHERE genre_name = ? AND song_id = ?;""",
            (genre_name, song_id)
        )

        if cursor.fetchone():
            logging.info("Genre {} with song_id {} already exists in genres table".format(
                genre_name, song_id))
            return False

        cursor.execute(
            """INSERT INTO genres (genre_name, song_id, dt_added)
            VALUES (?, ?, ?);""",
            (genre_name, song_id, datetime.datetime.now())
        )

        self.conn.commit()
        logging.info("Inserted genre {} with song_id {} into genres table".format(
            genre_name, song_id))
        return True

    # IMPORTANT: FUNCTION BELOW!

    def populate_database(self, soundfiles_path=config.SOUNDFILES_PATH):
        """
        populate_database Populate the database using the data from the soundfiles in the SOUNDFILES_PATH directory
        """

        # fetch all the files from config.SOUNDFILES_PATH
        soundfiles = []

        # there could be nested directories, so we need to recursively go through all the directories
        for root, dirs, files in os.walk(soundfiles_path):
            for file in files:
                soundfiles.append(os.path.abspath(os.path.join(root, file)))

        
        bar = Bar("Processing soundfiles", max=len(soundfiles))

        for file_path in soundfiles:
            # get path of file

            # get metadata from file
            parser = None
            try:
                parser = songparser.SongMetadata(filepath=file_path)
            except Exception as e:
                logging.error(f"Could not parse file: {file_path} with error: {e}")
                continue

            # get the song data and insert it into the database
            song_data = parser.get_song_table_data()
            song_id = "N/A"

            if song_data is not None:
                self.insert_song(**song_data)
                song_id = song_data["song_id"]
            else:
                logging.error(f"Could not get song data for file: {file_path}")
                continue

            # get the artist data and insert it into the database
            album_artist_data = parser.get_album_artist_data()
            if album_artist_data is not None:
                for artist in album_artist_data:
                    self.insert_album_artist(artist, song_id)

            song_artist_data = parser.get_song_artist_data()
            if song_artist_data is not None:
                for artist in song_artist_data:
                    self.insert_song_artist(artist, song_id)

            composer_data = parser.get_composer_data()
            if composer_data is not None:
                for composer in composer_data:
                    self.insert_composer(composer, song_id)

            genre_data = parser.get_genre_data()
            if genre_data is not None:
                for genre in genre_data:
                    self.insert_genre(genre, song_id)

            bar.next()
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    #                                  Backup and Restore
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    def backup_database(self) -> bool:
        """
        backup_database Backup the database to the path specified in config.DATABASE_BACKUP_PATH

        Returns:
            bool: True if successful, False if not
        """
        logging.info("Backing up database")

        # Zip the database file and name it with the current date
        # then move it to config.DATABASE_BACKUP_PATH
        # database is at config.DATABASE_PATH

        with zipfile.ZipFile(f"{config.DATABASE_BACKUP_PATH}/{datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.zip", 'w') as zip:
            zip.write(config.DATABASE_PATH, arcname="analytics.db")

        logging.info("Backed up database")
        return True


def init_db():
    # create an instance of the database handler
    db_handler = AnalyticsDBHandler()
    db_handler.create_all_tables()


if __name__ == "__main__":

    db_handler = AnalyticsDBHandler()
    db_handler.create_all_tables()
    db_handler.populate_database(soundfiles_path="C:\\Users\\drale\\Music\\music")
    # albums = db_handler.get_all_albums()