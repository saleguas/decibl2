The main file that contains all the configuration variables. Has tons of handy dandy info the rest of the program might need. Here's some important variables it contains:

### Logging

- `LOGGING_LEVEL` - The logging level of the project. This should be always be DEBUG, otherwise you might not get any logs
- `LOGGING_FORMAT` - The format of the logs. Those weird variables are the date, file location, and message.
- `LOGGING_DATE_FORMAT` - The format of the date in the logs.
- `LOGGING_FILENAME` - The filename of the logs. This is automatically generated based on the date.
- `LOGGING_ENCODING` - The encoding of the logs. This should always be utf-8.

- `DATABASE_PATH` - The path to the database. This is automatically generated based on the project's directory.

### Database

- `DATABASE_PATH` - The path to the database. This is automatically generated based on the project's directory.

### Sound Files

- `SOUNDFILES_PATH` - The path to the sound files. This is automatically generated based on the project's directory.

### Backups

- `BACKUPS_PATH` - The path to the backups. This is automatically generated based on the project's directory.
- `DATABASE_BACKUP_PATH` - The path to the database backups. This is automatically generated based on the project's directory.
- `LOGS_BACKUP_PATH` - The path to the logs backups. This is automatically generated based on the project's directory.

::: src.core.config
