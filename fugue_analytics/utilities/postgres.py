import psycopg2 as pg
import os

def connect_to_postgres():
    # Establishes connection to Postgres database
    conn = pg.connect(
        database="postgres", 
        user='postgres', 
        password=os.environ["PG_PASSWORD"], 
        host='34.121.37.113', 
        port='5432'
    )
    return conn

def execute_query(query=None):
    if not query:
        query = """
                SELECT * FROM metrics_over_time;
                """
    # Drops a table
    conn = connect_to_postgres()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

if __name__ == "__main__":
    connect_to_postgres()