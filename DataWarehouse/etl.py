import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries


def calculate_time(start, end):
    print(end - start)


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Started loading table')
        start = time.time()
        cur.execute(query)
        conn.commit()
        end = time.time()
        calculate_time(start, end)
        print('Loaded staging table')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Inserting into table ")
        start = time.time()
        cur.execute(query)
        conn.commit()
        end = time.time()
        calculate_time(start, end)
        print('Data inserted into table complete')


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("DROPPED TABLE ", query.split(" ")[-1])


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(*config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # drop_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

