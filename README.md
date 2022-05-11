# Sparkify ETL Pipeline for Redshift

Welcome to our Sparkify ETL pipeline documentation!

## About this project

This project enables analysts to access crucial data for understanding our users. It brings together user activity data from the Sparkify app (log_data) with details about our music catalog (song_data). Both of these datasets are extracted from the raw data files, transformed to make the data as useful as possible for analysis, and loaded into fact and dimension tables in a Redshift database to ensure efficient processing of analytic queries.

The Sparkify schema is designed to allow analysts to efficiently query the database while keeping the number of joins required to a minimum. Analysts can start their queries on the songplays table, joining the dimension tables as needed to get the information they need.

The ETL pipeline is a repeatable, reliable process for:

1. Moving data out of the difficult to work with JSON files in S3 into temporary tables.
2. Cleaning and transforming the data to the form expected by the analysts. For example, converting the epoch dates to a readable timestamp format that can be used in queries.
3. Inserting the cleaned data into a star schema that is used in the Sparkify data model.

## Getting started with the project

1. Clone or fork the repository.
2. Create and activate a virtual environment in the repository directory. For example:
   ```
    python3 -m venv .venv
    source .venv/bin/activate
   ```
3. Install the Python package requirements:
   ```
    python3 -m pip install -r requirements.txt
   ```
4. Update `dwh.cfg` with your credentials for Redshift and the role to use to read files from S3.
5. Run `create_tables.py`.
6. Run `etl.py`.

## Source data

The source data for this project is two collections of JSON files from the Sparkify music streaming app stored in AWS S3 buckets.

### song_data

A set of directories and JSON files with information about the songs available in the Sparkify app. This dataset is a subset of song data. Each file contains song and artist metadata.

Sample song record:

```
{
  "num_songs": 1,
  "artist_id": "ARULZCI1241B9C8611",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Luna Orbit Project",
  "song_id": "SOSWKAV12AB018FC91",
  "title": "Midnight Star",
  "duration": 335.51628,
  "year": 0
}
```

### log_data

A set of directories and JSON files that contain logs on user activity on the Sparkify app.

Sample log record:

```
{
    "artist":"Deas Vail",
    "auth":"Logged In",
    "firstName":"Elijah",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Davis",
    "length":237.68771,
    "level":"free",
    "location":"Detroit-Warren-Dearborn, MI",
    "method":"PUT",
    "page":"NextSong",
    "registration":1540772343796,
    "sessionId":985,
    "song":"Anything You Say (Unreleased Version)",
    "status":200,
    "ts":1543607664796,
    "userAgent":"\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4\"",
    "userId":"5"
}
```

## ETL pipeline files

The following files implement the extract, transform, load (ETL) pipeline for the Sparkify data model:

- `sql_queries.py` - Contains all of the SQL code used in the pipeline including using the COPY command to transfer data from S3 to the staging tables, and to create and drop the required tables and insert data into them.
- `create_tables.py` - Script to automate the process of deleting ETL tables from the previous run, copy data from S3, and create the required tables for the Sparkify data model.
- `etl.py` - Runs the ETL process to connect to the Redshift database, to copy data from S3 to the staging tables, and then to apply the required transformations and insert data into the finalized data model tables.
- `dwh.config`- Contains configuration settings for connecting to AWS Redshift and S3.

## Data Model

### Songplays

Fact table that contains a row for each song played.

| column      | type         | notes                |
| ----------- | ------------ | -------------------- |
| songplay_id | INT IDENTITY | Primary key.         |
| start_time  | TIMESTAMP    | Not null constraint. |
| user_id     | BIGINT       | Not null constraint. |
| level       | VARCHAR      |                      |
| song_id     | VARCHAR      |                      |
| artist_id   | VARCHAR      |                      |
| session_id  | INT          |                      |
| location    | VARCHAR      |                      |
| user_agent  | VARCHAR      |                      |

### Users

Dimension table that contains a row for each Sparkify user.

| column     | type    | notes        |
| ---------- | ------- | ------------ |
| user_id    | BIGINT  | Primary key. |
| first_name | VARCHAR |              |
| last_name  | VARCHAR |              |
| gender     | VARCHAR |              |
| level      | VARCHAR |              |

### Songs

Dimension table that contains a row for each song in the Sparkify catalog.

| column    | type    | notes                |
| --------- | ------- | -------------------- |
| song_id   | VARCHAR | Primary key.         |
| title     | VARCHAR | Not null constraint. |
| artist_id | VARCHAR | Not null constraint. |
| year      | INT     |                      |
| duration  | FLOAT   | Not null constraint. |

### Artists

Dimension table that contains a row for each artist in the Sparkify catalog.

| column    | type             | notes                |
| --------- | ---------------- | -------------------- |
| artist_id | VARCHAR          | Primary key.         |
| name      | VARCHAR          | Not null constraint. |
| location  | VARCHAR          |                      |
| latitude  | DOUBLE PRECISION |                      |
| longitude | DOUBLE PRECISION |                      |

### Time

Dimension table that contains a row for each timestamp, with columns with preprocessed dimensions including day, month, and year to simplify and optimize date-based analytic queries.

| column     | type      | notes        |
| ---------- | --------- | ------------ |
| start_time | TIMESTAMP | Primary key. |
| hour       | INT       |              |
| day        | INT       |              |
| week       | INT       |              |
| month      | INT       |              |
| year       | INT       |              |
| weekday    | INT       |              |

## Sample queries

1. Number of and percent of paid vs. free users by week

```
WITH free_vs_paid AS (
	SELECT
	  user_id,
	  start_time,
	  CASE WHEN "level" = 'paid' THEN 1 ELSE 0 END AS paid_user,
	  CASE WHEN "level" = 'free' THEN 1 ELSE 0 END AS free_user
	FROM songplays
)
SELECT
	week,
	SUM(paid_user) AS paid_users,
	SUM(free_user) AS free_users,
	ROUND(SUM(paid_user) * 100.0 / (SUM(free_user) + SUM(paid_user)), 1) as pct_paid
FROM free_vs_paid
INNER JOIN time ON free_vs_paid.start_time = time.start_time
GROUP BY week
ORDER BY week
```

2. Top 10 most active paid users by songs played

```
SELECT
first_name || ' ' || last_name AS username,
COUNT(songplay_id) AS songs_played
FROM songplays
INNER JOIN users ON songplays.user_id = users.user_id
WHERE users.level = 'paid'
GROUP BY user_name
ORDER BY songs_played DESC
LIMIT 10
```

3. Top 5 artists by number of songs played

```
SELECT
	name AS artist,
	COUNT(songplay_id) AS songs_played
FROM songplays
INNER JOIN artists ON songplays.artist_id = artists.artist_id
GROUP BY name
ORDER BY songs_played DESC
LIMIT 5
```
