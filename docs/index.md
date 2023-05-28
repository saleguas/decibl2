# decibl-docker

Welcome to the decibl-docker documentation!


## Installation

1. Install Python 3.10+
2. Run `pip install -r requirements.txt`

## File Structure

The Project is Structured as follows:

<!-- src/ has backups/ databases/ core/ logs/ soundfiles/ tests/ -->

```bash
src/
├── databases/
|── backups/
|── core/
|── logs/
|── soundfiles/
|── tests/
```

Let's go over what each one of them does and why they are important.

# src

Also called `source`. Where all the actual code is stored. This is the only folder that is actually required to run the project. All the files outside of this folder are for development purposes only.

Most large projects use a `src` folder.

## analyticsdb

**Note: This folder is generated automatically when the project is run.**

This folder contains the physical database file. This is where analyticsdb.py stores all the data/retrieves it, and is the overall "homebase" for all the analytics. Look at the analyticsdb.py file for more information.

## backups

**Note: This folder is generated automatically when the project is run.**

This folder contains the backups of the database and logs. It is automatically generated when the project is run, and is used to store backups of the database and logs. Look at the analyticsdb.py and config.py files for more information.

## core

This folder contains the core files of the project. These are the files that are used to run the project.

```bash
core/
├── analyticsdb.py
|── config.py
|── songparser.py
```

## logs

**Note: This folder is generated automatically when the project is run.**

This folder contains the logs of the project. It is automatically generated when the project is run, and is used to store logs of the project. Look at the config.py file for more information.

## soundfiles

This folder contains the physical sound files of the project. This is where the project gets the sound files from. Look at the songparser.py file for more information.

## tests

This folder contains the tests for the project. Run `pytest` to run the tests. Look at the tests for more information.

---
