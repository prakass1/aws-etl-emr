"""
This file represents all the queries executed to transform the data so that it can be utilized for the down stream activities
such as visualization, ml model building activities.
"""
## The year condition is added because there are some rows which have year as 0 and we want to avoid them.

songs_q="""
SELECT song_id, artist_id, title, year, duration
FROM songs_db
WHERE year != 0
"""

artists_q="""
SELECT sd.artist_id,
sd.artist_name as name,
sd.artist_location as location,
sd.artist_latitude as latitude,
sd.artist_longitude as longitude
FROM songs_db sd
"""

## The page condition is added as per the requirement beforehand here itself and not requiring to add it in the songplays.

logs_q="""
SELECT * FROM logs_db
WHERE page="NextSong"
"""

# Query for Users, handling duplicates through the idea based on the ts. Therefore, we get the latest modified information from the users.

users_q="""
with unique_staging_events as (
    SELECT userId, firstName, lastName, gender, level, ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
    FROM  nextPlay_logs_db
    WHERE userId IS NOT NULL
    )
    SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
    FROM unique_staging_events
    WHERE rank=1
"""

# Extracting time information from the time_db temp view.

time_q="""
SELECT timestamp as start_time, hour, dayofmonth as day, weekofyear as week, month, year, weekday
FROM time_db
"""

# Creating the fact table based on the time and song tables. The year and month is taken from time and added it into the songplays for partitioning.
# The condition to keep unique data by checking on song_title and also its duration.

songplays_q="""
SELECT row_number() OVER (PARTITION BY '' ORDER BY '') as songplay_id,
td.timestamp as start_time,
td.year as year,
td.month as month,
td.userId as user_id,
td.level,
st.song_id,
st.artist_id,
td.sessionId as session_id,
td.location,
td.userAgent as user_agent
FROM song_table st JOIN time_db td ON (st.title = td.song AND st.duration = td.length)
"""
