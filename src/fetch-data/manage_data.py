"""
- Generates description of given data and stores it in "../utils/[table_name]/[table_name]_desc.csv".
- Has few postgres functionality try outs.
"""

import sys

sys.path.append("../utils")
from imports import *
from postgres_utils import *
import fetch_data

with open("../utils/table_info.json", "r") as f:
    table_info_data = json.load(f)
ref_table = "forestcovertype"


def generate_data_desc():
    # Store Data description in a CSV
    data_desc = pd.DataFrame(fetch_data.df.describe())
    data_desc.to_csv(table_info_data[ref_table]["data_desc_csv_path"], index=False)
    return print("Desc CSV generated!")


pg = PostgresUtils()
conn, cursor, engine = pg.connection(
    table_info_data[ref_table]["db_conenction_string"],
    table_info_data[ref_table]["dbname"],
    table_info_data["user_info"]["user"],
    table_info_data["user_info"]["password"],
    table_info_data["user_info"]["host"],
    table_info_data["user_info"]["port"],
)

df_queries = pd.read_csv(
    table_info_data[ref_table]["test_query_file_path"],
    index_col=None,
)


def run_query():
    sql_query = 'SELECT * FROM forest_cover where "Slope" > 1000 and "Slope" < 3000'
    cursor.execute(sql_query)
    result = cursor.fetchall()
    return print(result[0][0])


def close_connection():
    pg.connectionClose(conn, cursor)


# run_query()
# close_connection()
