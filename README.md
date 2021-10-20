# aws_dwh
First DWH implementation with Amazon Redshift 

My dashboard

[<img src="images/power_bi.png" width="600">](https://app.powerbi.com/view?r=eyJrIjoiZjM5NTlmNzMtNjEyYy00YzgyLTk0YjgtNWFiOTJmZDVjZDc0IiwidCI6IjAyZDljYjNmLTFmZDMtNDQyMS05YjVkLTYwY2MxMzNhNTg3YSIsImMiOjJ9)

<h1>Data Engineering Nanodegree Program</h1>
<h2>Project 1: Data Modeling with Postgres</h2>
<h3>1. The purpose of the database in the context of srartup, Sparkify, and their analytical goals.</h3>

* The database:
    * Was created for the analysis of the Sparkify music streaming data logs on songs and user activity.
    * Since the analytics team is interested in the users' music choises, this database helps to perform such analysis. This relational database is a perfect solution for ease of quering the data as apposed to getting the data from the files stored in JSON logs.

***

<h3>2. State and justify your db schema desing and ETL pileline</h3>

 <h4>2.1 Schema design</h4>
 
* The following image is an ER diagram for the implemented **star schema**:
![Star Schema](images/star_schema.png)
    * The schema, consists of four **dimension tables** (`users`, `songs`, `artists`, `time`) and **fact table** `playsongs`. (An additional ER diagram could be created with `'er_diagram.py'` and it's output could be found in [sparkifydb_erd.png](sparkifydb_erd.png))
    * Each of the **dimension tables** has a **primary key** -- i.e., `users: `**`user_id`**, `songs: `**`song_id`**, `artists: `**`artist_id`**, and `time: `**`start_time`**. These **dimension tables** are referenced by the `songplays` table with the corresponding _foreign keys_(in italic). 
    * Additionally, each of the dimension table allows for a simple answer of the Sparkify's needs regarding the users and songs (and if needed artists and time details).
    * Specifically, the dimension tables have the following purposes:
      - `users` table keeps all information on the Sparkify users, including their names, gender and the level of subscription they have (free or paid). In addition, it is allowed to update that status (`level`). A new 'upsert' query would make an update on the level if it encounters an already existing `user_id`.
      - `artists` table keeps all information separately about the artists, including their name, location, and location's coordinates (latitude and longitude).
      - `songs` table hold specific information related to a song played, i.e., title of the song, name of the artist for that song, song's production year and duration.
      - `time` table holds all detailed information regarding the time stamp for the played song. All this information was extracted from the timestamp directly using the pandas datetime function, such as hour, day, week, month, year (as integers) and the day of the week the song was played.
    * In addition to it's **primary key** and the _foreign keys_ mentioned above, the fact table `songplays` also records the user's subscription level, location, session_id, and the user_agent information of user's system on which they played the song.
    * Such desing is perfect for the business problem. The denormalized tables allow for easy quering and fast aggregation of all needed information as well as help to perform easy joins. 

<h4>2.2 ETL Pipeline.</h4>

* Initially, the song data ([data/song_data](./data/song_data)) is processed by iterating the directory's json files and making insertions about each new song into the `artists` and `songs` tables. This happens in etl.py process_song_file() function.
* The log data ([data/log_data](./data/log_data)) is processed by iterating the directory's json files and making  the insertions of each log into the `users`, `time` tables, and, partially, into the `songplays` table (etl.py process_log_file() function).
* Both data directories are extracting the data by creating, and populating Pandas dataframes, after what the insertions are made into the relevant tables by executing  cur.execute(`TABLE_NAME`_table_insert, row) command.

<h4>2.3 Project Repository files</h4>

* : This section describes what files are for which purpose in the project 
- Run  [create_tables.py](./create_tables.py), to connect to the sparkify database, create the tables (or drop them and re-create new ones if existed).
- Then run [etl.py](./etl.py) to populate the tables with data and complete the ETL process. 
- To test the table's content run [test.ipynb](./test.ipynb). 
- The sparkifydb DDL could be found in [sql_queries.py](./sql_queries.py).

<h4>2.4 How To Run the Project</h4>

1. create_tables.py
2. etl.py
3. (optional checks) sql_queries.py

***
<h3>3. [Optional] Provide example queries and results for song play analysis.</h3>

* For example, Sporkify wants to know the proportion of users that pay for their service as apposed to those with free accounts. This could be done by a simple query to the `users` table:
    ```
    SELECT level, 
           count(*) / (SELECT count(*) FROM users)::float AS prcnt
    FROM users
    GROUP BY 1;
    ```
     Which produces the following output:
       
|_|level |prcnt |
|--|--|--|
1|free | 0.79167
2|paid | 0.20833

or

* Sporkify wants to know the top 10 locations their most loyal users spent their weekends in Novermer 2018. This could be simply answered by joining songplays and time tables with the following query:
   ```
   SELECT location, count(*) 
   FROM songplays s
   INNER JOIN time t ON t.start_time = s.start_time
   WHERE t.weekday in ('Saturday', 'Sunday') AND
         s.start_time BETWEEN '2018-11-01' AND '2018-12-01' 
   GROUP BY location
   ORDER BY count(*) DESC
   LIMIT 10;
   ```
   Which gives the following output:

|_|location | count |
|--|--|--|
1|Atlanta-Sandy Springs-Roswell, GA | 146
2|Tampa-St. Petersburg-Clearwater, FL |122
3|San Francisco-Oakland-Hayward, CA | 117
4|Winston-Salem, NC | 97
5|Portland-South Portland, ME | 90
6|Waterloo-Cedar Falls, IA | 90
7|Sacramento--Roseville--Arden-Arcade, CA | 57
8|Marinette, WI-MI | 31
9|Chicago-Naperville-Elgin, IL-IN-WI | 30
10|San Jose-Sunnyvale-Santa Clara, CA | 24



___

:bell: **Additional questions for the reviewer**

1) You said I could create an ER diagram. But I did create it before and included it into this readme file. Is it not good enough because it doesn't have information on what the primary keys are (bold font) and other additional information? 
2) You wrote "I would suggest using proper headings, emphasis, underline the relevant keywords. Use bullet points, add link URLs and images to make the README better." 
I have used the links to the images here, and the headers, and the bullet points. I guess I don't quite understand how this file is supposed to look like to pass your proper format standards. Is it possible to see some example of a perfectly formatted readme file? 
3) A question regarding the template file etl.py. Are we supposed to change its code completely so it's not considered a plagiarism? Or it is not expected for this project?
4) What kinds of data checks are you asking for in this projects as an extra to make this project to stand out?
5) What kind of dashboard are you taking about? Do you simply want an extra ipynb file with visualizations or do you actually want us to integrate some kind of BI tool (like Tableau) into this project? Would it be possible to get a link with examples please?
6) When you talk about the bulk insert of data, do you mean the direct insert from json file -> to postgres tables? Or do you mean converting JSON to CSV first and then use \copy? I've studied the link you shared with me earlier, but i'm still a little bit confused which method I should use here.

I would really appreciate if you comment on this questions! Thank you so much! 🙏🏻

