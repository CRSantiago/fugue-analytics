import psycopg2 as pg
import os
from sqlalchemy import create_engine

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

def insert_df_to_table(df, table):
    """
    Using cursor.executemany() to insert a dataframe
    Modified from: https://stackoverflow.com/a/70409917/11163214
    """
    conn = connect_to_postgres()
    cursor = conn.cursor()

    # Create a list of tuples from the dataframe values
    tuples = list(set([tuple(x) for x in df.to_numpy()]))

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (
        table, cols)

    try:
        cursor.executemany(query, tuples)
        conn.commit()

    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1

    finally:
        cursor.close()
        conn.close()
        
    return 

# if __name__ == "__main__":
#     insert_df_to_table()