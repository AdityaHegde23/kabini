import pandas as pd
from postgres_utils import *
import fetch_data

# db_conenction_string = "postgresql://postgres:postgres@localhost/forest-covertype-db"
# dbname = "forest-covertype-db"
# user = "postgres"
# password = "postgres"
# host = "localhost"
# port = "5432"
# test_query_file_path = (
#     "../test-queries/unique_postgres_forest_range_queries_5000_with_quotes.csv"
# )
# data_desc_csv_path = "../utils/forestcover/forest_covertype_desc.csv"


def generate_data_desc():
    # Store Data description in a CSV
    data_desc = pd.DataFrame(fetch_data.df.describe())
    data_desc.to_csv(data_desc_csv_path, index=False)
    return print("Desc CSV generated!")


pg = PostgresUtils()
conn, cursor, engine = pg.connection(
    db_conenction_string, dbname, user, password, host, port
)

df_queries = pd.read_csv(
    test_query_file_path,
    index_col=None,
)


def get_label():
    c = 0
    for row in df_queries.iloc[:, 0]:
        sql_query = row.replace("*", "count(*)")
        cursor.execute(sql_query)
        result = cursor.fetchall()
        if result[0][0]:
            c += 1
        print(result[0][0])
    print(c)
    return print("Labels created!")


def run_query():
    sql_query = (
        'SELECT * FROM forest_covertype_db where "Slope" > 1000 and "Slope" < 3000'
    )
    cursor.execute(sql_query)
    result = cursor.fetchall()
    return print(result[0][0])


pg.connectionClose(conn, cursor)
