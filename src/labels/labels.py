"""
- Fetches actual cardinality count of the given queries by running query in postgres.
- Stores labels against the queries in "../utils/[table_name]/[table_name]_labels.csv".
"""

import sys

sys.path.append("../utils")
sys.path.append("../fetch-data")
from imports import *
from postgres_utils import *

with open("../utils/table_info.json", "r") as f:
    table_info_data = json.load(f)
ref_table = "power"
label_path = f"../utils/{table_info_data[ref_table]['table_name']}/{table_info_data[ref_table]['table_name']}_labels.csv"
queries_path = f"../utils/{table_info_data[ref_table]['table_name']}/{table_info_data[ref_table]['table_name']}_queries.csv"

queries_df = pd.read_csv(queries_path)

pg = PostgresUtils()
conn, cursor, engine = pg.connection(
    table_info_data[ref_table]["db_conenction_string"],
    table_info_data[ref_table]["dbname"],
    table_info_data["user_info"]["user"],
    table_info_data["user_info"]["password"],
    table_info_data["user_info"]["host"],
    table_info_data["user_info"]["port"],
)


for index, row in queries_df.iterrows():
    query = row["queries"].replace("*", "count(*)")
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    queries_df.loc[index, "actual_value"] = result[0][0]

queries_df.to_csv(label_path)
print("Labels created!")
pg.connectionClose(conn, cursor)
