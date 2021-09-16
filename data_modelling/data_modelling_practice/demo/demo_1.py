import psycopg2

try:
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=postgres password=poklpokl"
    )
except psycopg2.Error as e:
    print("Error: Could not make connection to the postgres database")
    print(e)

conn.set_session(autocommit=True)
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursore to the database")
    print(e)

try:
    cur.execute("CREATE database udacity")
except psycopg2.Error as e:
    print(e)

try:
    conn.close()
except psycopg2.Error as e:
    print(e)

try:
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=udacity user=postgres password=poklpokl"
    )
except psycopg2.Error as e:
    print("Error: Could not make connection to the postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursore to the database")
    print(e)

try:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int);"
    )
except psycopg2.Error as e:
    print("Error: Issue creating table")

