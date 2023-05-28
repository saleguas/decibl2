from fastapi import Depends, FastAPI
from analyticsdb import init_db
from routers import album_artists,composers,genres,playlists_songs,playlists,plays,song_artists,songs,cloud_sync
from remote_tree import RemoteTree
import sys,os
import logging

app = FastAPI(
    dependencies=[Depends(init_db)]
)
logger = logging.getLogger("gunicorn.error")

os.chdir("../..")
remoteTree = RemoteTree("src/soundfiles")

app.include_router(album_artists.router)
app.include_router(composers.router)
app.include_router(genres.router)
app.include_router(playlists_songs.router)
app.include_router(playlists.router)
app.include_router(plays.router)
app.include_router(song_artists.router)
app.include_router(songs.router)
app.include_router(cloud_sync.router)
