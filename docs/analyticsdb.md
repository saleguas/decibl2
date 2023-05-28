### Overview

The main file that interacts with the database. Responsible for creating, adding, and retrieving data from the database. Here's some important infomation about the file:

This file interacts with the analytics database using a SQLite3 database. This is the main file that interacts with the database. It is responsible for creating, adding, and retrieving data from the database.

Create an AnalyticsDBHandler object and run commands to do stuff.

```py
dbhandler = AnalyticsDBHandler() # make the object
dbhandler.create_all_tables() # create all the tables
dbhandler.populate_database() # looks into soundfiles/ folder and adds all the info to the tables
dbhandler.get_song_by_id(1) # get the song with the id of 1
```

The file is split up into different sections, you'll see a large comment break between each section.

All the funtions run raw sqlite3, so there isn't a rigid structure to the database.

### Important Functions

#### create_all_tables()

**You have to create the tables before doing anything with the data!**

If you don't run this command (if the tables don't already exist), you'll get constant errors. Also once created, the data persists between runs of the program (they're stored in the file analyticsdb.db).

If you delete the file or it doesn't exist, run this command to create the tables.

#### populate_database()

The bread and butter of the file. This function looks into the soundfiles/ folder and adds all the info to the tables. _This includes adding data to multiple databases at once, it handles all the hard work for you._

Realistically, this is the only function we'll be needing to add data to the table.

It accounts for duplicates, so look for WARNING messages in the logs if you're adding a lot of data.

#### insert_song(\*\*kwargs)

Yeah, there's a ton of parameters. This function is used to insert a song into the database. It's a bit of a pain to use, but it's the only way to insert a song into the database. Look at songparser.py for more info.

::: src.core.analyticsdb
