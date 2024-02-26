"""
- Has postgres utils functions such as create connection, create table, close connection.
"""

import psycopg2
from sqlalchemy import create_engine


class PostgresUtils:

    def connection(self, db_connection_string, dbname, user, password, host, port):

        # Postgres

        # Connect to the PostgreSQL server
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port,  # Default PostgreSQL port
            )
        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL server:", e)

        # SQL Alchemy
        engine = create_engine(db_connection_string)

        cursor = conn.cursor()

        return conn, cursor, engine

    def createTable(self, table_name, data_frame, engine):
        data_frame.to_sql(table_name, engine, if_exists="replace", index=False)
        return print(f"{table_name} table created.")

    def connectionClose(self, conn, cursor):
        conn.close()
        cursor.close()
        return print("Conenction closed")
