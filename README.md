# decibl-docker
[![RelativelyBlank](https://circleci.com/gh/decibl/decibl-docker.svg?style=svg)](<LINK>)

The homeserver for the decibl service.

Read the documentation here:

https://decibl.github.io/decibl-docker/


# TODO list

1. ~~Add a way to query all songs in a playlist~~
2. ~~Add a way to query all songs by an artist~~
3. ~~Add a way to query all songs by a genre~~
4. ~~Only create tables if they don't already exist~~
5. Add more support for song files (mp3, wav, m4a, etc.)
6. ~~Add a way to add a song from a raw file~~
7. Add a way to remove an entry using a raw file
8. ~~Add CircleCI support~~
9. **ADD MORE UNIT TESTS!!**
10. Start resarching sync algorithms
11. Implement Sync algorithms
    - SYNC soundfiles (the actual sound data)
    - SYNC Analyticsdb (there will be an exac copy on the user's end, we want to add their data to our table and delete theirs)
12. Add the API
13. Add AUTH to the API
14. Fix the logs, add option to hold X amount of logs or delete logs older than X days or something so they dont get a gazillion bytes big
15. ~~Overall add a ton of useful database queries.~~
16. Implement "spotify wrapped" and other cool animated graphics with the data we have! (this will be very late down the line, but think about it still)
17. Implement the [linux-server.io S6 overlay for the docker container](https://github.com/just-containers/s6-overlay).
18. Add the docker container
19. Add automatic image building in circle-ci
