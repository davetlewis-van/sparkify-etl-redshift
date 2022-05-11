import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop existing tables created during previous ETL pipline runs.

    Parameters
    ----------

    cur : string
        The database cursor to use to run the queries
    conn: string
        The Redshift database connection.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create temporary tables and Sparkify star schema tables.
    
    Parameters
    ----------

    cur : string
        The database cursor to use to run the queries
    conn: string
        The Redshift database connection.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Create a connection to the `dwh` Redshift database.
    Drop any existing tables and create the temporary tables
    and Sparkify data model tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()