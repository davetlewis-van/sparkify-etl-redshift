import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies data from the S3 buckets to the Redshift 
    staging_events and staging_songs tables.
    
    Parameters
    ----------

    cur : string
        The database cursor used to insert records in the database.

    conn : string
        The connection string used to connect to the database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Selects data from the staging tables and inserts it into 
    the appropriate fact and dimension tables. 
    
    Parameters
    ----------

    cur : string
        The database cursor used to insert records in the database.

    conn : string
        The connection string used to connect to the database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Defines the database connection information and the tables to process.
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