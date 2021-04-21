import configparser
import psycopg2
import boto3
import json
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries
config = configparser.ConfigParser()
config.read('dwh.cfg')
print(config)



def drop_tables(cur, conn):
    """
    its to drop the tables in data base in order to avoid conflict create tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ calls create table quesries from sql_queries to creat tables in data base schema
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """main function calls all the function
       - calls drop function to drop tables if exists before.
       - calls create_tables to create all the tables
       - makes the ddatabase connection
    """

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(cur)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    
    
    


