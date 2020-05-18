# This project is part of Udacity's Nanodegree program "Data Engineering"

# Purpose

The purpose of this project is to support Sparkify by creating a datalake which enables them to perform an in-depth songplay analysis. The data sources are various json files residing in S3. The data will be loaded back into S3 as a set of dimensional tables.

# Data sources

Source data resides in S3 and comes in json files of the following formats:

## log_data (user attributes and activities like song plays)
```json

{ "artist": "N.E.R.D. FEATURING MALICE",
  "auth": "Logged In",
  "firstName": "Jayden",
  "gender": "M",
  "itemInSession": 0,
  "lastName": "Fox",
  "length": 288.9922,
  "level": "free",
  "location": "New Orleans-Metairie, LA",
  "method": "PUT",
  "page":"NextSong",
  "registration": 1541033612796.0,
  "sessionId": 184,
  "song": "Am I High (Feat. Malice)",
  "status": 200,
  "ts": 1541121934796,
  "userAgent": "\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
  "userId": 101
}
```

## song_data (known songs and their artists)
```json
{ "num_songs": 1, 
  "artist_id": "ARJIE2Y1187B994AB7", 
  "artist_latitude": null, 
  "artist_longitude": null, 
  "artist_location": "", 
  "artist_name": "Line Renaud", 
  "song_id": "SOUPIRU12A6D4FA1E1", 
  "title": "Der Kleine Dompfaff", 
  "duration": 152.92036, 
  "year": 0
}
```

# Schema

We follow a star-schema with songplay table being the fact table. This enables us to run performant queries and we are also able to set up meaningful queries with ease.

## Fact table

--### songplays (extracted from log files and linked to artists and songs)

|   Column    |  Type     |  Property   |
| ----------- | --------- | ----------- |
| songplay_id | integer   | Primary key |
| start_time  | timestamp |             |
| user_id     | integer   | Not Null    |
| level       | varchar   |             |
| song_id     | varchar   | Not Null    |
| artist_id   | varchar   | Not Null    |
| session_id  | integer   | Not Null    |
| location    | varchar   |             |
| user_agent  | varchar   |             |

## Dimension tables

### users (users in database)

|   Column    |  Type     |
| ----------- | --------- |
| user_id     | integer   |
| first_name  | string    |
| last_name   | string    |
| gender      | string    |
| level       | string    |

### songs (known songs)

|   Column    |  Type     |
| ----------- | --------- |
| song_id     | string    |
| title       | string    |
| duration    | double    |
| artist_id   | string    | used for partitioning
| year        | integer   | used for partitioning

### artists (known artists)

|   Column    |  Type     |
| ----------- | --------- |
| artist_id   | string    |
| name        | string    |
| location    | string    |
| latitude    | double    |
| longitude   | double    |

--### time (timestamps of song plays split into different units)

|   Column    |  Type     |
| ----------- | --------- |
| start_time  | timestamp |
| hour        | integer   |
| day         | integer   |
| week        | integer   |
| month       | integer   |
| year        | integer   |
| weekday     | integer   |

# How to run
Set AWS key and access id dl.cfg.
Then open a python console and call
``` 
run etl.py
```
to populate the data into tables and store them in S3.


# Files in repository
data/**/*                | Local test data
delFolders.ipynb         | Helper to delete local folder outputs after testing etl.py locally
dl.cfg                   | Configuration file to set AWS key and access id
etl.py                   | etl procedures to transform the data residing in S3 into the db schema and loading the results back into S3
print_schemes.ipynb      | Test function to view tables loaded back into S3 and printing their schemas.
README.md                | this file