# Project 3: Data Warehouse - DWH implementation with Amazon Redshift

## Table of contents:
1. [Summary](https://github.com/alena-c/aws_dwh/blob/main/README.md#1-summary) <br>
2. [Shema and ETL Pipeline](https://github.com/alena-c/aws_dwh/blob/main/README.md#2-state-and-justify-your-db-schema-design-and-etl-pileline)<br>
2.1 [Schema design](https://github.com/alena-c/aws_dwh/blob/main/README.md#21-schema-design-schema-for-song-play-analysis)<br>
2.2 [ETL Pipeline](https://github.com/alena-c/aws_dwh/blob/main/README.md#22-etl-pipeline)<br>
2.3 [Project Repository Files](https://github.com/alena-c/aws_dwh/blob/main/README.md#23-project-repository-files)<br>
2.4 [How to Run the Project](https://github.com/alena-c/aws_dwh/blob/main/README.md#24-how-to-run-the-project)<br>
3. [Optional Steps](https://github.com/alena-c/aws_dwh/blob/main/README.md#3-extra-steps)<br>
3.1 [Dashboard](https://github.com/alena-c/aws_dwh/blob/main/README.md#31-dashboard)<br>
3.2 [Query examples](https://github.com/alena-c/aws_dwh/blob/main/README.md#32-query-examples)

### 1. Summary
The purpose of the database in the context of srartup, Sparkify, and their analytical goals.

* The database:
    * Was created for the analysis of the Sparkify music streaming data logs on songs and user activity.
    * After Sparkify's users base and song database have grown in scale (and keeps growing), the decision to move their procceses and data to cloud sounds like a wise one - the previous single relational database might no longer be enough. 
    * Also, since the analytics team is interested in the users' music choises, a database stored in a data warehouse would seem like a more appropriate architecture for performing fast, easy to understand analytics and reports from the business perspective. 
    * Similarly, data warehouse is specifically structured for analitics. I's perfect for retrieving data from various sourses into a dimensional data store which further improves analytical query perfomance. The Sparkify's datasets already reside in Amazon S3, thus Amazon Redshift-hosted database is a convinient, and optimal solution for all previously listed purposes.

***
 
### 2. State and justify your db schema design and ETL pileline
#### 2.1 Schema design (Schema for Song Play Analysis)
  
* The following image is an ER diagram for the implemented **star schema**:
![Star Schema](images/star_schema.png)
    * This dimensional schema, consists of four **dimension tables** (`users`, `songs`, `artists`, `time`) and a **fact table** `playsongs`. Each of the dimension table allows for a convenient analytics for the Sparkify's needs regarding the users and songs (and if needed artists and time details).
    * Specifically, the dimension tables have the following purposes:
      - `users` table keeps all information on the Sparkify users, including their names, gender and the level of subscription they have (free or paid). Here, the table would countain duplicate values for some `user_id`'s since their subscription `level` can chang, but all privious `level` info is kept in the database. This is useful for users' membership level analysis.
      - `artists` table keeps all information separately about the artists, including their name, location, and location's coordinates (latitude and longitude).
      - `songs` table hold specific information related to a song played, i.e., title of the song, name of the artist for that song, song's production year and duration.
      - `time` table holds all detailed information regarding the time stamp for the played song. All this information was extracted from the timestamp directly using the pandas datetime function, such as hour, day, week, month, year (as integers) and the day of the week the song was played.
    * Fact table `songplays` records the user's subscription level, location, session_id, and the user_agent information of user's system on which they played the song.
    * The star schema tables are populated from the staging tables. Such dimensional design is perfect for the business problem. Keeping the staging tables with source data would allow to change the design of analytics tables, thus staying flexible for changing business needs. Whereas dimentional tables allow for an easy and performant analytics application and fast retrieval of all needed information. 
    * Since Redshift doesn't enforce  the **primary/foreign key** constraints, a good practise is to disactivate those constraints all together. That should imporove OLAP querying performance.
    * The analytics tables should be optimized depending on the queries specified by the analytics team. Thus the distribution styles and sorting keys in the dimensional and fact tables should be assigned only after the query is specified; otherwise, the performace could suffer. Here, since the queries were not provided, I wrote up several queries myself and tested which the distribution styles and sorting keys for which fields would optimize the performance of these queries. Although I must mention, that for our toy project the default destibution style (EVEN) and no sorting keys would not perform much worse than my optimized solution. In real world of big data, more testing would be nessessary to determine the constraints. 
  
#### 2.2 ETL Pipeline

In short, an ETL pipeline extracts the data from S3, stages it in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to.

* Sparkify's two datasets, song and log data, reside in [S3](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend).
* Initially, the song data (s3://udacity-dend/song_data) and log data (s3://udacity-dend/log_data) are loaded from S3 into staging tables on Redshift with `load_stagin_tables()` function in `etl.py`. (((by bulk COPY command the directory's json files and making insertions about each new song into the `artists` and `songs` tables.))) 
* Staging tables data, in turn, populate the analytics tables. This is done by the `etl.py`'s `insert_tables()` function.

#### 2.3 Project Repository Files

* : This section describes what files are for which purpose in the project 
- [create_tables.py](./create_tables.py) connects to the redshift database, creates the tables (or drop them and re-create new ones if existed). Allows to reset the database and test ETL pipeline. (You can use Query Editor in the AWS Redshift console for testing if this worked).
- [etl.py](./etl.py) loads data from Amazon S3 into staging tables and then process that data into the analytics tables on Amazon Redshift. (and complete the ETL process. ) (The script connects to the Sparkify redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables.)
- [sql_queries.py](./sql_queries.py) creates database DDL, defines SQL statements, which will be imported into the two files above.
- [dwh.cfg](./dwh.cfg) configuration file for the above three files. Redshift database and IAM role info should be added before running `create_tables.py`.
- [README.md](./README.md) is where you'll provide discussion on your process and decisions for this ETL pipeline.

Extra files:
- [create_cluster.py](./create_cluster.py) launches a Redshift cluster and creates an IAM role that has read access to S3.
- [dwh_start.cfg](./dwh_start.cfg) configuration file for `create_cluster.py`.
- [check_cluster_status.ipynb](./check_cluster_status.ipynb) checks cluster's status and opens a TCP port.
- [view_datasets.ipynb](./view_datasets.ipynb) shows datasets that reside on S3. 
- [test.ipynb](./test.ipynb) queries sample data from the analytics tables. 
- [delete_cluster.ipynb](./delete_cluster.ipynb) deletes the Redshift cluster when finished.

#### 2.4 How To Run the Project

_With existing cluster_:
1. create_tables.py
2. etl.py
3. (optional checks) sql_queries.py

_If launching a cluster_:
1. create_cluster.py
2. check_cluster_status.ipynb
3. (optional) view_datasets.ipynb
4. create_tables.py
5. etl.py
6. (optional checks) sql_queries.py
7. (optional) test.ipynb
8. delete_cluster.ipynb


***
### 3. Extra Steps
#### 3.1 Dashboard
Here is my dashboard for analytic queries (made with Microsoft Power BI)*:

[<img src="images/power_bi.png" width="600">](https://app.powerbi.com/view?r=eyJrIjoiZjM5NTlmNzMtNjEyYy00YzgyLTk0YjgtNWFiOTJmZDVjZDc0IiwidCI6IjAyZDljYjNmLTFmZDMtNDQyMS05YjVkLTYwY2MxMzNhNTg3YSIsImMiOjJ9)

*Note: For this dashboard, I used the larger dataset `song-data` also available on S3.

#### 3.2 Query examples

* Sporkify wants to know the top 10 songs which are most popular among their users on the weekends:
   ```
   SELECT s.title, count(*)
   FROM songplays sp
   JOIN songs s ON s.song_id = sp.song_id
   JOIN time t ON sp.start_time = t.start_time
   WHERE t.weekday in ('Saturday', 'Sunday')
   GROUP BY s.title
   ORDER BY 2 DESC
   LIMIT 10;
   ```
   Which gives the following output:

|_|title | count |
|--|--|--|
1|You're The One | 3
2|Nothin' On You [feat. Bruno Mars] (Album Version) |2
3|Caught Up In You | 2
4|Up Up & Away | 2
5|Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | 2
6|From The Ritz To The Rubble | 2
7|Never Saw It Coming | 1
8|Beautiful | 1
9|Let's Get It Started | 1
10|Yippiyo-Ay | 1

