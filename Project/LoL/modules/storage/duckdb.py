# DuckDB 관련 기능 함수
import duckdb
import subprocess
import os
from pathlib import Path
from urllib.request import urlretrieve


# Base Functions
def get_connection(db_path=None):
    conn = duckdb.connect(db_path) if db_path else duckdb.connect(":memory:")
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


def docker_build_metabase():
    # docker build metabase/. --tag metaduck:latest
    subprocess.run(
        ["docker", "build", "../../metabase/.", "--tag", "metaduck:latest"],
        check=True
    )
        

def docker_run_metabase(container_name="metabase-duck", port=3000):
    base_dir = Path(__file__).parent / "metabase"
        
    def run(cmd):
        print(f"$ {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    
    result = subprocess.run(
        ["docker", "ps", "-a", "-q", "-f", f"name={container_name}"],
        capture_output=True,
        text=True
    )
    if result.stdout.strip():
        # run(["docker", "rm", "-f", container_name])
        print(f"[INFO] Container {container_name} already exists. Starting it.")
    else:
        print(f"[INFO] Starting new container {container_name}.")
        run([
            "docker", "run", "-d", "--name", container_name,
            "-p", f"{port}:3000",
            "-e", "MB_PLUGINS_DIR=/home/plugins",
            "-v", f"{base_dir}/data:/home/data",
            f"metaduck:latest"
        ])


def create_insert_query(table_name, columns):
    # 참고: DuckDB는 Python API에서 %s 대신 ? 또는 :param 형태의 placeholder를 사용합니다.
    query = f"""
    INSERT OR REPLACE INTO {table_name} ({", ".join(columns)})
    VALUES ({", ".join(["?"] * len(columns))});
    """
    return query


# if __name__ == "__main__":
#     conn = get_connection("outputs/duckdb.db")
#     df = conn.execute("SELECT * FROM read_json_auto('modules/data_ingestion/output/league.json')").fetchdf()
#     print(df)
