import configparser
import psycopg2
from sql_queries_test_no_dups import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load data from S3 into staging tables on Redshift
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Insert data into analytics tables on Redshift
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Load data from S3 into staging tables on Redshift and then process 
    that data into analytics tables on Redshift
    '''
    config = configparser.ConfigParser()
    config.read('dwh_cluster.cfg')      # TODO change back to dwh.cfg before committing 


    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
