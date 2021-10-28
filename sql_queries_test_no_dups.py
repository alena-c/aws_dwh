import configparser


# CONFIG
config = configparser.ConfigParser()
# config.read('dwh.cfg')
config.read('dwh_cluster.cfg')      # TODO delete this and uncomment line 6 before committing 

DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

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
                                 level      char(4))
                                 diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id   varchar       not null SORTKEY DISTKEY,
                                 title     varchar(500)  not null,
                                 artist_id varchar       not null,
                                 year      int, 
                                 duration  numeric(18,5) not null);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id varchar       not null SORTKEY, 
                                   name      varchar(500),
                                   location  varchar(500),
                                   latitude  numeric(9,5), 
                                   longitude numeric(9,5))
                                   diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(start_time timestamp   not null SORTKEY,
                                hour int, 
                                day int,
                                week int, 
                                month int, 
                                year int, 
                                weekday varchar)
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
                                    user_agent  varchar);
""")


# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
timeformat as 'epochmillisecs'
format as json {};
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song-data'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2'
""").format(DWH_ROLE_ARN)

# FINAL TABLES

# will fill users with duplicate values for user_id, fname, lname, gender, where some level changes, but all privious level info is kept
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
    WHERE page = 'NextSong';
""")

# will make a table with duplicated values for song_id, title, artist_id and year where only duration differs
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
    FROM staging_songs;
""")

# --select count(*) from songs --> 384995
# --SELECT DISTINCT song_id FROM songs --> 291272

# -- if the following query returns 70790 rows, then how could table song contain 384995 rows?!
# --SELECT DISTINCT   song_id,
# --                  title,
# --                  artist_id,
# --                  year,
# --                  duration
# --FROM staging_songs;  -->70790, 70765 -- every time different number, why??

                                
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
    FROM staging_songs;
""")

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
    WHERE page = 'NextSong';
""")

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
    ON (se.artist, se.song, se.length) = (ss.artist_name, ss.title, ss.duration) --> makes 6500 rows
    --ON (se.artist, se.song) = (ss.artist_name, ss.title) --> makes 6962 rows
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]