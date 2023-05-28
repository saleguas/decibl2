# File that takes system variables and puts them into python variables
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime

# ---------------------------------------------------------------------------------------------
#                                      Logging
# ---------------------------------------------------------------------------------------------

LOGGING_LEVEL = logging.DEBUG
# For logging format, do datetime + file location then message
LOGGING_FORMAT = "%(asctime)s - %(pathname)s - %(levelname)s - %(message)s"
LOGGING_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
# make filename based on date
LOGGING_FILENAME = os.path.abspath(os.path.join(os.path.dirname(
    __file__), "..", "logs", "log_{}.log".format(datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S"))))
LOGGING_ENCODING = "utf-8"

if not os.path.exists(os.path.dirname(LOGGING_FILENAME)):
    os.makedirs(os.path.dirname(LOGGING_FILENAME))


logging.basicConfig(filename=LOGGING_FILENAME, encoding=LOGGING_ENCODING,
                    level=LOGGING_LEVEL, format=LOGGING_FORMAT, datefmt=LOGGING_DATE_FORMAT)
logging.debug("Making folder for logs")


logging.info("Loading config file")
logger = logging.getLogger("Rotating Time Log")
handler = TimedRotatingFileHandler(LOGGING_FILENAME,
                                   when="h",
                                   interval=1,)
logger.addHandler(handler)

# log a message that the config file has been loaded
logging.info("Loaded config file")

# make logging folder if it doesn't exist

# ---------------------------------------------------------------------------------------------
#                                      Database
# ---------------------------------------------------------------------------------------------

DATABASE_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "databases", "analytics.db"))

DATABASE_TEST_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "tests", "analytics.db"))
# make database folder if it doesn't exist
logging.info("Making folder for database")
if not os.path.exists(os.path.dirname(DATABASE_PATH)):
    os.makedirs(os.path.dirname(DATABASE_PATH))

ZIPPED_DATABASE_TEST_PATH1 = os.path.abspath(os.path.join(
    os.path.dirname(DATABASE_TEST_PATH), "analyticsdb.zip"))
# ---------------------------------------------------------------------------------------------
#                                      Sound Files
# ---------------------------------------------------------------------------------------------

# make sound folder if it doesn't exist
logging.info("Making folder for sound files")
SOUNDFILES_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "soundfiles"))
if not os.path.exists(SOUNDFILES_PATH):
    os.makedirs(SOUNDFILES_PATH)

song_table_data = {
    "song_id": "",  # string
    "filepath": None,  # string
    "main_artist": None,  # string
    "filesize": 0,  # int in bytes
    "padding": None,  # int in bytes
    "album_artwork_bit_depth": None,  # int in bits
    "album_artwork_colors": None,  # int
    "album_artwork_height": None,  # int in pixels
    "album_artwork_width": None,  # int in pixels
    "bit_depth": None,  # int in bits
    "bitrate": None,  # int in bits, divide by 1000 to get Kbps
    "channels": None,  # int
    "duration": None,  # int in seconds
    "sample_rate": None,  # int in KHz
    "album": None,  # string
    "barcode": None,  # string
    "date_created": None,  # string in YYYY-MM-DD
    "disc_number": None,  # int
    "disc_total": None,  # int
    "isrc": None,  # string
    "itunesadvisory": None,  # string
    "length": None,  # int
    "publisher": None,  # string
    "rating": None,  # int
    "title": "Missing",  # string
    "track_number": None,  # int
    "track_total": None,  # int
    "source": None,  # string
}

# ---------------------------------------------------------------------------------------------
#                                      Backups
# ---------------------------------------------------------------------------------------------

BACKUPS_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "backups"))
DATABASE_BACKUP_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "backups", "database"))
LOGS_BACKUP_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "backups", "logs"))
# make backup folder if it doesn't exist
logging.info("Making folder for backups")
if not os.path.exists(BACKUPS_PATH):
    os.makedirs(BACKUPS_PATH)
if not os.path.exists(DATABASE_BACKUP_PATH):
    os.makedirs(DATABASE_BACKUP_PATH)
if not os.path.exists(LOGS_BACKUP_PATH):
    os.makedirs(LOGS_BACKUP_PATH)
