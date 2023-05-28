# Make a class to parse the metadata of song Files
#import audio_metadata
import config
import os
from abc import ABC, abstractmethod
import logging
import audio_metadata
from typing import Any, List, Dict
import hashlib
# make decorator to log a certain method is being ran with a file
# this is the first time I've used decorators LOL

def file_to_hash(filepath: str) -> str:
    """
    file_to_hash Get the SHA256 hash of a file.

    Args:
        filepath (str): Path to the file.

    Returns:
        str: The SHA256 hash of the file.
    """    

    # Open,close, read file and calculate SHA256 on its contents
    with open(filepath, "rb") as f:
        bytes = f.read()  # read entire file as bytes
        readable_hash = hashlib.sha256(bytes).hexdigest();
        return readable_hash

def log_data(func):
    def wrapper(*args, **kwargs):
        logging.info("Running " + func.__name__ + args[0].filename if args[0].filename else args[0].filepath)
        return func(*args, **kwargs)
    return wrapper


class SongFile(ABC):
    """
    SongFile The core class for parsing metadata from song files

    Args:
        ABC (_type_): Abstract Base Class
    """    

    

    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        logging.info("Parsing metadata for " + self.filename)

    def __init__(self):
        self.filepath = None
        self.filename = None

    @log_data
    @abstractmethod
    def load_file(self, filepath) -> None:
        """
        load_file Abstract method to load a file

        Args:
            filepath (str): the path to the file
        """        
        pass

    @log_data
    @abstractmethod
    def get_song_table_data(self) -> Dict[str, str]:
        """
        get_song_table_data Abstract method to get all the data required for the song table

        Returns:
            Dict: a dictionary with all the data required for the song table
        """        
        pass

    @log_data
    @abstractmethod
    def get_album_artist_data(self) -> List[str]:
        """
        get_album_artist_data Abstract method to get all the data required for the album artist table

        Returns:
            List[str]: a list of all the album artists
        """        
        pass

    @log_data
    @abstractmethod
    def get_song_artist_data(self) -> List[str]:
        """
        get_song_artist_data Abstract method to get all the data required for the song artist table

        Returns:
            List[str]: a list of all the song artists
        """        
        pass

    @log_data
    @abstractmethod
    def get_composer_data(self) -> List[str]:
        """
        get_composer_data Abstract method to get all the data required for the composer table

        Returns:
            List[str]: a list of all the composers
        """        
        pass

    @log_data
    @abstractmethod
    def get_genre_data(self) -> List[str]:
        """
        get_genre_data Abstract method to get all the data required for the genre table

        Returns:
            List[str]: a list of all the genres
        """        
        pass


class SongFileFLAC(SongFile):

    # def __init__(self, filepath: str):
    #     """
    #     __init__ constructor for SongFileFLAC

    #     Args:
    #         filepath (str): the filepath of the FLAC song file (absolute path, use os.join)
    #     """        
    #     super().__init__(filepath)
    #     self.metadata = None
    #     self.song_table_data = None

    #     self.loadMetadata(filepath)
    #     self.make_song_table_data()

    def __init__(self):
        """
        __init__ Default constructor for SongFileFLAC. Does nothing.
        """        
        super().__init__()
        self.metadata = None
        self.song_table_data = None
        self.make_song_table_data()

    def load_file(self, filepath:str) -> None:
        """
        load_file Loads the metadata of the FLAC file into the metadata variable.

        Args:
            filepath (str): the filepath of the FLAC song file (absolute path, use os.join)

        Returns:
            _type_: None
        """        
        self.hash = file_to_hash(filepath)
        self.metadata = audio_metadata.load(filepath)

    def loadMetadataParams(self, params: dict) -> None:
        """
        loadMetadataParams Loads the metadata of the FLAC file into the metadata variable. Necessary if not using filepaths.

        Args:
            params (dict): the metadata of the FLAC song file
        """        
        self.song_table_data = params



    def make_song_table_data(self):
        """
        make_song_table_data Instantiates the song_table_data dictionary with all the keys and values set to None.
        """        
        # there's so much data bruh, here's a big ass list that details everything
        # this method isn't really necessary, but I thinik it makes things cleaner
        self.song_table_data = config.song_table_data.copy()

    def get_song_table_data(self) -> Dict[str, str]:
        """
        get_song_table_data Looks at self.metadata and self.song_table_data and grabs all possible data from self.metadata and puts it into self.song_table_data.

        Returns:
            Dict: a dictionary with all the data required for the song table   
        """        
        super().get_song_table_data()
        self.song_table_data['song_id'] = self.hash
        # do the above but with if statements to check if the key exists
        if "filepath" in self.metadata:
            self.song_table_data["filepath"] = self.metadata["filepath"]
        if "filesize" in self.metadata:
            self.song_table_data["filesize"] = self.metadata["filesize"]
        if "padding" in self.metadata:
            self.song_table_data["padding"] = self.metadata["padding"].size
        if "pictures" in self.metadata and len(self.metadata["pictures"]) > 0:
            if "bit_depth" in self.metadata["pictures"][0]:
                self.song_table_data["album_artwork_bit_depth"] = self.metadata["pictures"][0]["bit_depth"]
            if "colors" in self.metadata["pictures"][0]:
                self.song_table_data["album_artwork_colors"] = self.metadata["pictures"][0]["colors"]
            if "height" in self.metadata["pictures"][0]:
                self.song_table_data["album_artwork_height"] = self.metadata["pictures"][0]["height"]
            if "width" in self.metadata["pictures"][0]:
                self.song_table_data["album_artwork_width"] = self.metadata["pictures"][0]["width"]
        if "streaminfo" in self.metadata:
            if "bit_depth" in self.metadata["streaminfo"]:
                self.song_table_data["bit_depth"] = self.metadata["streaminfo"]["bit_depth"]
            if "bitrate" in self.metadata["streaminfo"]:
                self.song_table_data["bitrate"] = self.metadata["streaminfo"]["bitrate"]
            if "channels" in self.metadata["streaminfo"]:
                self.song_table_data["channels"] = self.metadata["streaminfo"]["channels"]
            if "duration" in self.metadata["streaminfo"]:
                self.song_table_data["duration"] = self.metadata["streaminfo"]["duration"]
            if "sample_rate" in self.metadata["streaminfo"]:
                self.song_table_data["sample_rate"] = self.metadata["streaminfo"]["sample_rate"]
        if "tags" in self.metadata:
            if "album" in self.metadata["tags"]:
                self.song_table_data["album"] = self.metadata["tags"]["album"][0]
            if "barcode" in self.metadata["tags"]:
                self.song_table_data["barcode"] = self.metadata["tags"]["barcode"][0]
            if "date" in self.metadata["tags"]:
                self.song_table_data["date_created"] = self.metadata["tags"]["date"][0]
            if "discnumber" in self.metadata["tags"]:
                self.song_table_data["disc_number"] = self.metadata["tags"]["discnumber"][0]
            if "disctotal" in self.metadata["tags"]:
                self.song_table_data["disc_total"] = self.metadata["tags"]["disctotal"][0]
            if "isrc" in self.metadata["tags"]:
                self.song_table_data["isrc"] = self.metadata["tags"]["isrc"][0]
            if "itunesadvisory" in self.metadata["tags"]:
                self.song_table_data["itunesadvisory"] = self.metadata["tags"]["itunesadvisory"][0]
            if "length" in self.metadata["tags"]:
                self.song_table_data["length"] = self.metadata["tags"]["length"][0]
            if "publisher" in self.metadata["tags"]:
                self.song_table_data["publisher"] = self.metadata["tags"]["publisher"][0]
            if "rating" in self.metadata["tags"]:
                self.song_table_data["rating"] = self.metadata["tags"]["rating"][0]
            if "title" in self.metadata["tags"]:
                self.song_table_data["title"] = self.metadata["tags"]["title"][0]
            if "tracknumber" in self.metadata["tags"]:
                self.song_table_data["track_number"] = self.metadata["tags"]["tracknumber"][0]
            if "tracktotal" in self.metadata["tags"]:
                self.song_table_data["track_total"] = self.metadata["tags"]["tracktotal"][0]
            if "source" in self.metadata["tags"]:
                self.song_table_data["source"] = self.metadata["tags"]["source"][0]
        if "artist" in self.metadata["tags"]:
            self.song_table_data["main_artist"] = self.metadata["tags"]["artist"][0]

        return self.song_table_data

    def get_album_artist_data(self) -> List[str]:
        """
        get_album_artist_data gets the album artist data from the metadata

        Returns:
            List[str]: list of album artists for FLAC files
        """        
        super().get_album_artist_data()
        if "albumartist" in self.metadata["tags"]:
            return self.metadata["tags"]["albumartist"]
        else:
            return None

    def get_song_artist_data(self) -> List[str]:
        """
        get_song_artist_data gets the song artist data from the metadata

        Returns:
            List[str]: list of song artists for FLAC files
        """        
        
        super().get_song_artist_data()
        if "artist" in self.metadata["tags"]:
            return self.metadata["tags"]["artist"]
        else:
            return None

    def get_composer_data(self) -> List[str]:
        """
        get_composer_data gets the composer data from the metadata

        Returns:
            List[str]: list of composers for FLAC files
        """        
        super().get_composer_data()
        if "composer" in self.metadata["tags"]:
            return self.metadata["tags"]["composer"]
        else:
            return None

    def get_genre_data(self) -> List[str]:
        """
        get_genre_data gets the genre data from the metadata

        Returns:
            List[str]: list of genres for FLAC files
        """        
        super().get_genre_data()
        if "genre" in self.metadata["tags"]:
            return self.metadata["tags"]["genre"]
        else:
            return None

class SongFileMP3(SongFile):

    def __init__(self):
        super().__init__()
        self.make_song_table_data()

    def load_file(self, filepath:str) -> None:
        """
        load_file Loads the metadata of the FLAC file into the metadata variable.

        Args:
            filepath (str): the filepath of the FLAC song file (absolute path, use os.join)

        Returns:
            _type_: None
        """        
        
        self.metadata = audio_metadata.load(filepath)
        self.hash = file_to_hash(filepath)

    def make_song_table_data(self):
        """
        make_song_table_data makes the song table data from the metadata
        """        
        
        self.song_table_data = config.song_table_data.copy()

    def get_song_table_data(self) -> Dict[str, Any]:
        """
        get_song_table_data gets the song table data from the metadata

        Returns:
            Dict[str, Any]: dictionary of song table data
        """        
        super().get_song_table_data()



    #         MP3({
    #     'filepath': 'c:\\Users\\drale\\Documents\\GitHub\\decibl-docker\\src\\soundfiles\\mp3_with_art.mp3',
    #     'filesize': '7.98 MiB',
    #     'pictures': [
    #         <ID3v2Picture({
    #             'data': '1.10 MiB',
    #             'description': '',
    #             'height': 1200,
    #             'mime_type': 'image/jpeg',
    #             'type': <ID3PictureType.COVER_FRONT>,
    #             'width': 1200,
    #         })>,
    #     ],
    #     'streaminfo': <MP3StreamInfo({
    #         'bitrate': '245 Kbps',
    #         'bitrate_mode': <MP3BitrateMode.ABR>,
    #         'channel_mode': <MP3ChannelMode.JOINT_STEREO>,
    #         'channels': 2,
    #         'duration': '03:56',
    #         'layer': 3,
    #         'protected': False,
    #         'sample_rate': '44.1 KHz',
    #         'version': 1,
    #     })>,
    #     'tags': <ID3v2Frames({
    #         'album': ['Bakusou Yumeuta'],
    #         'artist': ["Diggy-MO'"],
    #         'comment': [
    #             <ID3v2Comment({
    #                 'description': '',
    #                 'language': 'eng',
    #                 'text': 'Download at: https://www.yumeost.club/',
    #             })>,
    #         ],
    #         'date': ['2008'],
    #         'discnumber': ['1'],
    #         'genre': ['Anime'],
    #         'rating': [
    #             <ID3v2Popularimeter({'count': 0, 'email': 'MusicBee', 'rating': 242})>,
    #         ],
    #         'title': ['Bakusou Yumeuta (\u7206\u8d70\u5922\u6b4c)'],
    #         'tracknumber': ['1'],
    #     })>,
    # })>
        self.song_table_data["song_id"] = self.hash
        if "filepath" in self.metadata:
            self.song_table_data["filepath"] = self.metadata["filepath"]
        if "filesize" in self.metadata:
            self.song_table_data["filesize"] = self.metadata["filesize"]
        if len(self.metadata["pictures"]) > 0:
            self.song_table_data["album_artwork_height"] = self.metadata["pictures"][0]["height"]
            self.song_table_data["album_artwork_width"] = self.metadata["pictures"][0]["width"]
        if "streaminfo" in self.metadata:
            self.song_table_data["bitrate"] = self.metadata["streaminfo"]["bitrate"]
            self.song_table_data["channels"] = self.metadata["streaminfo"]["channels"]
            self.song_table_data["duration"] = self.metadata["streaminfo"]["duration"]
            self.song_table_data["sample_rate"] = self.metadata["streaminfo"]["sample_rate"]
        if "tags" in self.metadata:
            if "album" in self.metadata["tags"]:
                self.song_table_data["album"] = self.metadata["tags"]["album"][0]
            if "artist" in self.metadata["tags"]:
                self.song_table_data["main_artist"] = self.metadata["tags"]["artist"][0]
            if "date" in self.metadata["tags"]:
                self.song_table_data["date_created"] = self.metadata["tags"]["date"][0]
            if "discnumber" in self.metadata["tags"]:
                self.song_table_data["disc_number"] = self.metadata["tags"]["discnumber"][0]
            if "title" in self.metadata["tags"]:
                self.song_table_data["title"] = self.metadata["tags"]["title"][0]
            if "tracknumber" in self.metadata["tags"]:
                self.song_table_data["track_number"] = self.metadata["tags"]["tracknumber"][0]
        
        if self.song_table_data["title"] is None:
            self.song_table_data["title"] = self.song_table_data["filepath"].split("\\")[-1].split(".")[0]
        if self.song_table_data["main_artist"] is None:
            self.song_table_data["main_artist"] = "Unknown Artist"
        return self.song_table_data

    def get_album_artist_data(self) -> List[str]:
        """
        get_album_artist_data gets the album artist data from the metadata

        Returns:
            List[str]: list of album artists for FLAC files
        """        
        super().get_album_artist_data()
        if "albumartist" in self.metadata["tags"]:
            return self.metadata["tags"]["artist"]
        else:
            return None

    def get_song_artist_data(self) -> List[str]:
        """
        get_song_artist_data gets the song artist data from the metadata

        Returns:
            List[str]: list of song artists for FLAC files
        """        
        
        super().get_song_artist_data()
        if "artist" in self.metadata["tags"]:
            return self.metadata["tags"]["artist"]
        else:
            return None

    def get_composer_data(self) -> List[str]:
        return None

    def get_genre_data(self) -> List[str]:
        """
        get_genre_data gets the genre data from the metadata

        Returns:
            List[str]: list of genres for FLAC files
        """        
        super().get_genre_data()
        if "genre" in self.metadata["tags"]:
            return self.metadata["tags"]["genre"]
        else:
            return None
    
    def __str__(self) -> str:
        return self.get_song_table_data().__str__()

    def __repr__(self) -> str:
        return self.get_song_table_data().__repr__()

class SongMetadata:
    # Songs have a lot of Metadata! We want to store as much as possible.
    # We will store the following:
    #   - filepath
    #   - filesize (in bytes)
    #   - padding (in bytes)
    #   - Album Artwork Bit Depth (in bits)
    #   - Album Artwork Colors (int)
    #   - Album Artwork Size (in bytes)
    #   - Album Artwork Height (in pixels)
    #   - Album Artwork Width (in pixels)
    #   - bit_depth (in bits)
    #   - bitrates (in Kbps)
    #   - channels (int)
    #   - duration (in seconds)
    #   - sample_rate (in KHz)
    #   - Album
    #   - Album Artists
    #   - Song Artists
    #   - Barcode
    #   - Composers
    #   - Date Created (in YYYY-MM-DD)
    #   - Disc Number
    #   - Disc Total
    #   - ISRC
    #   - itunesadvisory
    #   - length
    #   - publisher
    #   - rating
    #   - title
    #   - track number
    #   - track total
    #   - favorited (bool)

    

    def __init__(self, filepath=None):
        """Initialize the SongMetadata object.
            We want to see what file type it is, and load the correct file."""
        if filepath != None:
            self.extension = os.path.splitext(filepath)[1]
            self.songfile = None
            if self.extension == ".flac":
                self.songfile = SongFileFLAC()
                self.songfile.load_file(filepath)
            elif self.extension == ".mp3":
                self.songfile = SongFileMP3()
                self.songfile.load_file(filepath)
            else:
                logging.error("File type not supported: " + self.extension)


    def get_song_table_data(self) -> Dict[str, str]:
        """
        get_song_table_data Gets the required song data for inserting into the database depending on the file

        Returns:
            Dict[str, str]: Dictionary of song data
        """         
        if self.songfile is not None:
            return self.songfile.get_song_table_data()

    def get_album_artist_data(self) -> List[str]:
        """
        get_album_artist_data Gets the album artist data for inserting into the database depending on the file

        Returns:
            List[str]: List of album artists
        """        
        if self.songfile is not None:
            return self.songfile.get_album_artist_data()

    def get_song_artist_data(self) -> List[str]:
        """
        get_song_artist_data Gets the song artist data for inserting into the database depending on the file

        Returns:
            List[str]: List of song artists
        """        
        if self.songfile is not None:
            return self.songfile.get_song_artist_data()

    def get_composer_data(self) -> List[str]:
        """
        get_composer_data Gets the composer data for inserting into the database depending on the file

        Returns:
            List[str]: List of composers
        """        
        if self.songfile is not None:
            return self.songfile.get_composer_data()

    def get_genre_data(self) -> List[str]:
        """
        get_genre_data Gets the genre data for inserting into the database depending on the file

        Returns:
            List[str]: List of genres
        """        
        if self.songfile is not None:
            return self.songfile.get_genre_data()

    # when printed, print the metadata

    def __str__(self):
        return str(self.songfile.metadata)

    # when printed, print the metadata
    def __repr__(self):
        return str(self.songfile.metadata)


# md = SongMetadata(os.path.join(config.SOUNDFILES_PATH, "enemy.flac"))
# print(md.get_song_table_data())
# print(md.get_album_artist_data())
# print(md.get_song_artist_data())

# md2 = SongMetadata(os.path.join(config.SOUNDFILES_PATH, "gemstone.flac"))
# print(md2)
# print(md2.get_song_table_data())
# metadata2 = audio_metadata.load(os.path.join(config.SOUNDFILES_PATH, "mp3_with_art.mp3"))
# print(metadata2['pictures'][0]['height'])
# print(metadata2)

