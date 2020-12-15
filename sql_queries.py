# DROP TABLES

user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"

# CREATE TABLES


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id    int PRIMARY KEY,
                            first_name text,
                            last_name  text,
                            gender     text,
                            level      text ); """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id   text PRIMARY KEY,
                            title     text,
                            artist_id text,
                            year      int,
                            duration  decimal ); """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id text PRIMARY KEY,
                                name      text,
                                location  text,
                                latitude  float,
                                longitude float
                            ); """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp PRIMARY KEY,
                            hour       int,
                            day        int,
                            week       int,
                            month      int,
                            year       int,
                            weekday    int 
                        ); """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id serial PRIMARY KEY,
                                start_time timestamp, 
                                user_id int, 
                                level text, 
                                song_id text,
                                artist_id text, 
                                session_id int,
                                location text, 
                                user_agent text,
                                CONSTRAINT user_id
                                    FOREIGN KEY (user_id)
                                        REFERENCES users(user_id),
                                CONSTRAINT song_id
                                    FOREIGN KEY (song_id)
                                        REFERENCES songs(song_id),
                                CONSTRAINT artist_id
                                    FOREIGN KEY (artist_id)
                                        REFERENCES artists(artist_id),
                                CONSTRAINT ts
                                    FOREIGN KEY (start_time)
                                        REFERENCES time(start_time)); """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id , session_id , location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) 
DO UPDATE SET level = EXCLUDED.level;""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s,%s,%s,%s,%s) 
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s,%s,%s,%s,%s) 
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = (""" SELECT song_id,  a.artist_id FROM songs s 
    JOIN 
        artists  a ON s.artist_id = a.artist_id 
    WHERE 
      s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY EXAMPLES

COUNT_WEEKDAY = """ SELECT case when weekday = 0 then 'Sunday'
                                when weekday = 1 then 'Monday'
                                when weekday = 2 then 'Tuesday'
                                when weekday = 3 then 'Wednesday'
                                when weekday = 4 then 'Thursday'
                                when weekday = 5 then 'Friday'
                                when weekday = 6 then 'Saturday' end as weekday, 
                           COUNT(songplay_id) songs 
                    FROM songplays s 
                    JOIN time t ON s.start_time = t.start_time 
                    GROUP BY 1 
                    ORDER BY songs DESC"""

AMOUNT_SONG_GENDER = """ SELECT case when gender = 'F' then 'Woman' else 'Man' end as gender, COUNT(*) 
                         FROM songplays s 
                         JOIN users u ON s.user_id = u.user_id 
                         GROUP BY 1 
                         ORDER BY gender DESC"""

DURATION_ARTIST = """ SELECT a.name, SUM(duration) 
                      FROM songplays s 
                      JOIN artists a ON s.artist_id = a.artist_id 
                      JOIN songs sg ON sg.song_id = s.song_id 
                      GROUP BY 1 """

LEVEL_SONG = """ SELECT level, COUNT(*) status 
                 FROM songplays 
                 GROUP BY 1 
                 ORDER BY status DESC"""



# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]