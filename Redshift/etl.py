import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Extract data from S3 bucket and load into staging tables
    
    Parameters:
    copy_table_queries -- queries to extract from various bucket
                          locations into various loading tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from the staging tables into the final tables
    
    Parameters:
    insert_table_queries -- queries to select data from loading tables
                            and insert into final tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    Connect to the Redshift cluster and load data into it

    Parameters: 
    dwh.cfg (file): Login credentials

    Returns: 
    nothing 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()