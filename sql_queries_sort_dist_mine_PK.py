import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
#LOG_DATA = config.get("S3","LOG_DATA")
#SONG_DATA = config.get("S3","SONG_DATA")
#LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# (should i make location, artist, and song 500 charater-long here as well?)
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(event_id      bigint  IDENTITY(0,1) not null,
                                          artist        varchar,
                                          auth          varchar,
                                          firstName     varchar,
                                          gender        char(1),
                                          itemInSession int,
                                          lastName      varchar,
                                          length        numeric(18,5),
                                          level         char(4),
                                          location      varchar,
                                          method        varchar(7),
                                          page          varchar,
                                          registration  numeric,
                                          sessionId     int                  not null,
                                          song          varchar,
                                          status        int,
                                          ts            bigint               not null,
                                          userAgent     varchar,
                                          userId        int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(num_songs        int,
                                         artist_id        varchar        not null,
                                         artist_latitude  numeric(9,5),
                                         artist_longitude numeric(9,5),
                                         artist_location  varchar(500),
                                         artist_name      varchar(500),
                                         song_id          varchar        not null,
                                         title            varchar(500),
                                         duration         numeric(18,5),
                                         year             int);
""")

# diststyle all because it's a small table --only 312 records
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id    int      not null SORTKEY,
                                 first_name varchar, 
                                 last_name  varchar,
                                 gender     char(1), 
                                 level      char(4),
                                 primary key(user_id)
                                 )
                                 diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id   varchar       not null SORTKEY DISTKEY,
                                 title     varchar(500)  not null,
                                 artist_id varchar       not null,
                                 year      int, 
                                 duration  numeric(18,5) not null,
                                 primary key(song_id)
                                 );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id varchar       not null SORTKEY, 
                                   name      varchar(500),
                                   location  varchar(500),
                                   latitude  numeric(9,5), 
                                   longitude numeric(9,5),
                                   primary key(artist_id)
                                   )
                                   diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(start_time timestamp   not null SORTKEY,
                                hour int, 
                                day int,
                                week int, 
                                month int, 
                                year int, 
                                weekday varchar,
                                primary key(start_time)
                                )
                                diststyle all;
""")

# Fact Table

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(songplay_id int identity(0,1)  not null SORTKEY,
                                    start_time  timestamp not null,
                                    user_id     int       not null,
                                    level       char(4),
                                    song_id     varchar                DISTKEY,
                                    artist_id   varchar,
                                    session_id  int,
                                    location    varchar,
                                    user_agent  varchar,
                                    primary key(songplay_id),
                                    foreign key(start_time) references time(start_time),
                                    foreign key(user_id)  references users(user_id),
                                    foreign key(song_id) references songs(song_id),
                                    foreign key(artist_id) references artists(artist_id)
                                   );
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log-data'
credentials 'aws_iam_role={}'
region 'us-west-2'
timeformat as 'epochmillisecs'
format as json 's3://udacity-dend/log_json_path.json';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song-data/A/B/C'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2'
""").format(DWH_ROLE_ARN)

# FINAL TABLES

# with or without AND in WHERE clause the query returns 104 rows
user_table_insert = ("""
    INSERT INTO users(user_id, 
                      first_name, 
                      last_name, 
                      gender, 
                      level)
    SELECT DISTINCT   userid   as user_id,
                      firstname as first_name,
                      lastname as last_name,
                      gender,
                      level
    FROM staging_events
    WHERE page = 'NextSong'
        AND user_id NOT IN (SELECT DISTINCT user_id FROM users);
    --ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

# distinct = 23
song_table_insert = ("""
    INSERT INTO songs(song_id, 
                      title, 
                      artist_id, 
                      year, 
                      duration)
    SELECT DISTINCT   song_id,
                      title,
                      artist_id,
                      year,
                      duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
    --ON CONFLICT (song_id) DO NOTHING;
""")
                                
# distinct = 23
artist_table_insert = ("""
    INSERT INTO artists(artist_id, 
                        name, 
                        location, 
                        latitude,
                        longitude)
    SELECT DISTINCT     artist_id,
                        artist_name     as name,
                        artist_location as location,
                        artist_latitude as latitude,
                        artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
    --ON CONFLICT (artist_id) DO NOTHING;
""")

# distinct = 23
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
                    EXTRACT(hour FROM start_time)    as hour,
                    EXTRACT(day FROM start_time)     as day,
                    EXTRACT(week FROM start_time)    as week,
                    EXTRACT(month FROM start_time)   as month,
                    EXTRACT(year FROM start_time)    as year,
                    to_char(start_time, 'Day')       as weekday
    FROM staging_events
    WHERE page = 'NextSong' 
        AND start_time NOT IN (SELECT DISTINCT start_time FROM time);
    --ON CONFLICT (start_time) DO NOTHING;
""")

# distinct = 8023
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id,
                       session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time, 
                    se.userid       as user_id, 
                    se.level, 
                    ss.song_id, 
                    ss.artist_id, 
                    se.sessionid       as session_id, 
                    se.location,   
                    se.useragent    as user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON (se.artist, se.song, se.length) = (ss.artist_name, ss.title, ss.duration) 
    WHERE se.page = 'NextSong' 
        AND start_time NOT IN (SELECT DISTINCT start_time FROM songplays);
    --ON CONFLICT (songplay_id) DO NOTHING;
""")
# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
create_table_queries = [staging_events_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = [staging_events_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_queries = [staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

################################DELETE############################
# FIND SONGS
# implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and duration of a song.

#song_select = ("""
#    SELECT s.song_id, a.artist_id
#    FROM songs s
#    INNER JOIN artists a ON s.artist_id =  a.artist_id
#    WHERE s.title = %s AND a.name = %s AND s.duration = %s
#""")

# select count(song_id) from songs; --> 384995 
# but
# select distinct song_id from songs; --> 291272

# Here are some notes about SORTKEY AND DISTKEY:

    # Pick a few important queries you want to optimize your databases for. You can’t optimize your table for all queries, unfortunately.
    # To avoid a large data transfer over the network, define a DISTKEY.
    # From the columns used in your queries, choose a column that causes the least amount of skew as the DISTKEY. A column which has many distinct values, such as timestamp, would be a good first choice. Avoid columns with few distinct values, such as credit card types, or days of week.
    # Even though it will almost never be the best performer, a table with no DISTKEY/SORTKEY is a decent all-around performer. It’s a good option not to define DISTKEY and SORTKEY until you really understand the nature of your data and queries.