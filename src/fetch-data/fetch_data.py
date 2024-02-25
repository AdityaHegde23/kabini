import pandas as pd
from ucimlrepo import fetch_ucirepo
from postgres_utils import *

# fetch dataset from ucimlrepo
covertype = fetch_ucirepo(id=31)

# data (as pandas dataframes)
X = pd.DataFrame(covertype.data.features)
Y = pd.DataFrame(covertype.data.targets)
df = X.iloc[:, :10]  # final needed df

db_conenction_string = "postgresql://postgres:postgres@localhost/forest-covertype-db"
dbname = "forest-covertype-db"
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"
test_query_file_path = "../test-queries/unique_postgres_forest_range_queries_5000_with_quotes.csv"

pg = PostgresUtils()
conn, cursor, engine = pg.connection(db_conenction_string, dbname, user, password, host, port)

df_queries = pd.read_csv(
    test_query_file_path,
    index_col=None,
)
# print(df_queries)
c = 0
for row in df_queries.iloc[:, 0]:
    sql_query = row.replace("*", "count(*)")
    cursor.execute(sql_query)
    result = cursor.fetchall()
    if result[0][0]:
        c += 1
    print(result[0][0])
print(c)
# sql_query = 'SELECT * FROM forest_covertype_db where "Slope" > 1000 and "Slope" < 3000'
# cursor.execute(sql_query)
# result = cursor.fetchall()
# print(result[0][0])


# Create a cursor object
# cursor = conn.cursor()
# sql_query = "SELECT * FROM forest_covertype_db LIMIT 10"
# cursor.execute(sql_query)
# result = cursor.fetchone()
pg.connectionClose(conn, cursor)


# Close cursor and connection
# cursor.close()
