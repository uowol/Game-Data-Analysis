# DuckDB 관련 기능 함수
import duckdb

def connect_db(db_path):
    conn = duckdb.connect(db_path)
    return conn


if __name__ == "__main__":
    conn = connect_db("outputs/duckdb.db")
    df = conn.execute("SELECT * FROM read_json_auto('modules/data_ingestion/output/league.json')").fetchdf()
    print(df)