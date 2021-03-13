import configparser
import psycopg2
import time
from sql_queries import create_table_queries, drop_table_queries
from etl import calculate_time


def drop_tables(cur, conn):
    for query in drop_table_queries:
        start = time.time()
        cur.execute(query)
        conn.commit()
        end = time.time()
        calculate_time(start, end)
        print("DROPPED TABLE ", query.split(" ")[-1])


def create_tables(cur, conn):
    for query in create_table_queries:
        start = time.time()
        cur.execute(query)
        conn.commit()
        end = time.time()
        calculate_time(start, end)
        print("CREATED TABLE : ", query.split(" ")[2])


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
