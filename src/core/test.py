import concurrent.futures
import os
import logging
from progress.bar import Bar
from songparser import SongMetadata
import config
from tqdm import tqdm
from analyticsdb import *

def fetch_files(soundfiles_path):
    # Fetch all the files from the given path.
    soundfiles = []
    for root, dirs, files in os.walk(soundfiles_path):
        for file in files:
            soundfiles.append(os.path.abspath(os.path.join(root, file)))
    return soundfiles

def process_file(file_path, conn):
    db_handler = AnalyticsDBHandler(conn=conn)
    # Parse and process each individual file.
    try:
        parser = SongMetadata(filepath=file_path)
    except Exception as e:
        logging.error(f"Could not parse file: {file_path} with error: {e}")
        return

    # Process the data from the file.
    song_data = parser.get_song_table_data()
    song_id = "N/A"

    if song_data is not None:
        db_handler.insert_song(**song_data)
        song_id = song_data["song_id"]
    else:
        logging.error(f"Could not get song data for file: {file_path}")
        return

    album_artist_data = parser.get_album_artist_data()
    if album_artist_data is not None:
        for artist in album_artist_data:
            insert_album_artist(artist, song_id)

    song_artist_data = parser.get_song_artist_data()
    if song_artist_data is not None:
        for artist in song_artist_data:
            insert_song_artist(artist, song_id)

    composer_data = parser.get_composer_data()
    if composer_data is not None:
        for composer in composer_data:
            insert_composer(composer, song_id)

    genre_data = parser.get_genre_data()
    if genre_data is not None:
        for genre in genre_data:
            insert_genre(genre, song_id)

from tqdm import tqdm

def populate_database(soundfiles_path=config.SOUNDFILES_PATH):
    conn = sqlite3.connect(config.DATABASE_PATH, check_same_thread=False)

    # Fetch the files.
    soundfiles = fetch_files(soundfiles_path)

    # Process the files using multithreading.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_file, soundfile, conn) for soundfile in soundfiles}
        pbar = tqdm(total=len(soundfiles), desc="Processing Files")
        for future in concurrent.futures.as_completed(futures):
            pbar.update(1)
        pbar.close()
    conn.close()




if __name__ == "__main__":

    db_handler = AnalyticsDBHandler()
    db_handler.create_all_tables()
    populate_database(soundfiles_path="C:\\Users\\drale\\Music\\music")
    # albums = db_handler.get_all_albums()