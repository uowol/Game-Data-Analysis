import os
import re
import psycopg2


# Base Functions
def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname=os.getenv("DB_NAME", "dbname"),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PSWD", "password"),
    )
    conn.autocommit = True
    return conn


def excute_query(conn, query, params=None):
    cur = conn.cursor()
    if params:
        cur.execute(query, params)
    else:
        cur.execute(query)
    res = None
    try:
        res = cur.fetchall()
    except Exception as e:
        pass
    cur.close()
    return res


# Custom Functions
def is_safe_db_name(name):
    return re.match(r"^[a-zA-Z0-9_]+$", name) is not None


def ls_table(conn, verbose=False):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    tables = excute_query(conn, query)
    if verbose:
        print(tables)
    return tables


def create_insert_query(table_name, columns, primary_keys):
    query = f"""
    INSERT INTO {table_name} ({", ".join(columns)})
    VALUES ({", ".join(["%s"] * len(columns))})
    ON CONFLICT ({", ".join(primary_keys)}) DO NOTHING;
    """
    return query
