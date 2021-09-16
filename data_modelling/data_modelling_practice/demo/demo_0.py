import psycopg2


conn = psycopg2.connect(
    "host=127.0.0.1 dbname=studentdb user=postgres password=poklpokl"
)
cur = conn.cursor()
cur.execute("CREATE TABLE test123 (col1 int, col2 int, col3 int);")

conn = cur.execute("select count(*) from test123")
print(cur.fetchall())
