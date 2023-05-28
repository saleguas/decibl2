## Understanding the documentation Structure.

The config is in a file at the root called mkdocs.yml. This file contains all the information about the documentation. It contains the pages, the theme, and the plugins. The pages are the files that are used to generate the documentation. The theme is the theme of the documentation. 

```bash
/ # Root of the project
├── docs/ # Contains all the documentation files
|   ├── index.md # The main page of the documentation
|   ├── docinstructions.md # This file
|   ├── <other random .md files>
├── mkdocs.yml # The config file for mkdocs
├── src/
├── <other random files>
```

All the actual "meat" of the documentation is in the docs folder. This folder contains all the files that are used to generate the documentation. The files are in markdown format.


## Documenting your code

Use docstrings in your code, this may look like:

```py
def get_all_playlist_songs(self) -> List[dict]:
    """
    get_all_playlist_songs Get all the playlist songs in the database, returns a list of dictionaries

    Returns:
        List[dict]: list of dictionaries [song_name, file_size, id]
    """
    pass
```

or like

```py
def loadMetadataParams(self, params: dict) -> None:
    """
    loadMetadataParams Loads the metadata of the FLAC file into the metadata variable. Necessary if not using filepaths.

    Args:
        params (dict): the metadata of the FLAC song file
    """     
    pass
```

I highly encourage you to download a vs code extension to do this. I use [this one](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring) for python.

## Adding the document to the documentation

Make a new doc in the `docs` folder. Name it something similar to the file you are documenting. For example, if you are documenting `songparser.py`, name the file `songparser.md`.

Add whatever information you think is important in the file, **then make sure to add a special signature that lets mkdocs read your code!**

This signature will ALWAYS be `src.[folder in src].[file name]` with three colons in front (:::). Look below for example. (I cant add it here otherwise it gets overwritten.)

Some examples:

```md
::: src.core.analyticsdb
::: src.core.songparser
::: src.tests.test_analyticsdb
```

## Adding the document to the documentation index

Open the mkdocs.yml file and edit the `nav` tree. For example, it may look something like this.

```yml
nav: # DONT CHANGE THIS
  - Home: index.md # DONT CHANGE THIS
  - "Core Modules": # PARENT OF DROP DOWN
      - "analyticsdb.py": "analyticsdb.md" # NAME OF DROP DOWN ITEM: NAME OF DOCUMENT
      - "config.py": "config.md" # PAGE WILL APPEAR AS CONFIG.PY, AND RENDER DOCUMENT CONFIG.MD
      - "songparser.py": "songparser.md"
```

if I wanted to add this document per say, I would add:

```yml
nav: # DONT CHANGE THIS
  - Home: index.md # DONT CHANGE THIS
  - "Core Modules": # PARENT OF DROP DOWN
      - "analyticsdb.py": "analyticsdb.md" # NAME OF DROP DOWN ITEM: NAME OF DOCUMENT
      - "config.py": "config.md" # PAGE WILL APPEAR AS CONFIG.PY, AND RENDER DOCUMENT CONFIG.MD
      - "songparser.py": "songparser.md"
      - "newdocument.py": "newdocument.md"
```

## Push to gh-pages

Run the command

```bash
mkdocs serve
mkdocs gh-deploy
```