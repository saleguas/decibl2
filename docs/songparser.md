Responsible for parsing the song files and extracting their metadata.

### Overview

We are using an abstract class `SongFile` which should be extended by child classes for each file type. For example:

```python
# This is what the parent class looks like
class SongFile(ABC): # ABC means it's an abstract class
    @abstractmethod
    def get_song_table_data(self):
        pass

# This is the class we want to make
class SongFileFLAC(SongFile): # inherit from SongFile

    def get_song_table_data(self): # this is an abstract method, so we have to implement it
        super().get_song_table_data() # call the parent method (named the exact same as current function)
        pass

class SongFileMP3(SongFile): # inherit from SongFile

    def get_song_table_data(self): # this is an abstract method, so we have to implement it
        super().get_song_table_data() # call the parent method (named the exact same as current function)
        pass
```

If you are adding support for a new file, there's some very important things you have to be aware of:

1. Firstly, you need to create a child class of `SongFile` for your file type. Look at the above example for more information.

2. Add your filetype to the `__init__()` function of the SongMetadata class.

```python

    # This is what the init function will look like (maybe a little different since this was written but it will be same gist)

    def __init__(self, filepath):
        """Initialize the SongMetadata object.
            We want to see what file type it is, and load the correct file."""
        self.extension = os.path.splitext(filepath)[1]
        self.songfile = None
        if self.extension == ".flac":
            self.songfile = SongFileFLAC(filepath)

        # ----------------------------------------------#
        # Add your file type here! For example, for mp3:#
        # ----------------------------------------------#

        elif self.extension == ".mp3":
            self.songfile = SongFileMP3(filepath) # change this to the class you made
        else:
            logging.error("File type not supported: " + self.extension)

```

3. Add the required methods. These are the following
   - `get_song_table_data(self)`
   - `get_album_artist_data(self)`
   - `get_song_artist_data(self)`
   - `get_composer_data(self)`
   - `get_genre_data(self)`

Here's the actual hard part, you have to **return certain types of data for each of these methods**

#### get_song_table_data(self)

This one has the most information. Here's the list of stuff you have to return

```python
"main_artist": None, # string
"filesize": 0, # int in bytes
"padding": None, # int in bytes
"album_artwork_bit_depth": None, # int in bits
"album_artwork_colors": None, # int
"album_artwork_height": None, # int in pixels
"album_artwork_width": None, # int in pixels
"bit_depth": None, # int in bits
"bitrate": None, # int in bits, divide by 1000 to get Kbps
"channels": None, # int
"duration": None, # int in seconds
"sample_rate": None, # int in KHz
"album": None, # string
"barcode": None, # string
"date_created": None, # string in YYYY-MM-DD
"disc_number": None, # int
"disc_total": None, # int
"isrc": None, # string
"itunesadvisory": None, # string
"length": None, # int
"publisher": None, # string
"rating": None, # int
"title": "Missing", # string
"track_number": None, # int
"track_total": None, # int
"source": None, # string
```

It HAS to be a dict with all these keys, if you don't have the value for one, set it to `None`.

#### get_album_artist_data(self)

This one is a little easier. Just return a list of the album artists

```python
["artist1", "artist2", "artist3"]
```

#### get_song_artist_data(self)

Same thing as above, but for song artists

```python
["artist1", "artist2", "artist3"]
```

#### get_composer_data(self)

Same thing as above, but for composers

```python
["artist1", "artist2", "artist3"]
```

#### get_genre_data(self)

Same thing as above, but for genres

```python
["genre1", "genre2", "genre3"]
```

::: src.core.songparser
