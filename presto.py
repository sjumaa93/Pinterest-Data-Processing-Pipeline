from gettext import Catalog
import prestodb

connection = prestodb.dbapi.connect(
    host='localhost',
    catalog='cassandra',
    user='Saif Jumaa',
    port=8080,
    schema='data',
)

cur = connection.cursor()
cur.execute('SELECT * FROM data.pinterest')
rows = cur.fetchall()