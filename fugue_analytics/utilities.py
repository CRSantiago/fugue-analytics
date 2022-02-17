import psycopg2 as pg
import os

def connect_to_postgres():
    # Establishes connection to Postgres database
    conn = pg.connect(
        database="postgres", 
        user='postgres', 
        password=os.environ["PG_PASSWORD"], 
        host='35.192.179.194', 
        port='5432'
    )
    return conn