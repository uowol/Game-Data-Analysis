# DuckDB 관련 기능 함수
import duckdb


# Base Functions
def get_connection(db_path=None):
    conn = duckdb.connect(db_path) if db_path else duckdb.connect(':memory:')
    return conn

def excute_query(conn, query, params=None):
    if params:
        conn.execute(query, params)
    else:
        conn.execute(query)
    res = None
    try:
        res = conn.fetchall()
    except Exception as e:
        pass
    return res


# Custom Functions
def ls_table(conn, verbose=False):
    query = "SELECT table_name FROM duckdb_tables();"
    tables = excute_query(conn, query)
    if verbose:
        print(tables)
    return tables


def create_insert_query(table_name, columns):
    # 참고: DuckDB는 ON CONFLICT 구문을 지원하지 않고, Python API에서 %s 대신 ? 또는 :param 형태의 placeholder를 사용합니다.
    query = f"""
    INSERT OR REPLACE INTO {table_name} ({", ".join(columns)})
    VALUES ({", ".join(["?"] * len(columns))});
    """
    return query


# if __name__ == "__main__":
#     conn = get_connection("outputs/duckdb.db")
#     df = conn.execute("SELECT * FROM read_json_auto('modules/data_ingestion/output/league.json')").fetchdf()
#     print(df)
