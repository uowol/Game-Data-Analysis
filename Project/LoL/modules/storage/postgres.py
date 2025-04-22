import os
import re
import psycopg2
from psycopg2.extensions import AsIs


def is_safe_db_name(name):
    return re.match(r'^[a-zA-Z0-9_]+$', name) is not None


def ls_table(conn):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    cur = conn.cursor()
    cur.execute(query)
    tables = cur.fetchall()
    print(tables)
    cur.close()
    

def create_database(conn, db_name):
    assert is_safe_db_name(db_name), "허용되지 않는 데이터베이스 이름입니다."
    cur = conn.cursor()
    cur.execute("CREATE DATABASE %s;", (AsIs(db_name),))
    cur.close()    


def drop_database(conn, db_name):
    assert is_safe_db_name(db_name), "허용되지 않는 데이터베이스 이름입니다."
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS %s;", (AsIs(db_name),))
    cur.close()


def create_table(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    
    
def insert_data(conn, table_name, columns, data, primary_keys):
    query = f"""
    INSERT INTO {table_name} ({", ".join(columns)})
    VALUES ({", ".join(["%s"] * len(columns))})
    ON CONFLICT ({", ".join(primary_keys)}) DO NOTHING;
    """
    # print(f"[INFO] \n{query}\n{data}")
    
    cur = conn.cursor()    
    cur.execute(query, data)
    cur.close()
    

def init_database(conn):
    create_table(conn, """
    CREATE TABLE IF NOT EXISTS summoner (
        summoner_id VARCHAR PRIMARY KEY,
        puuid VARCHAR,
        game_name VARCHAR,
        game_tag VARCHAR
    );""")
    create_table(conn, """
    CREATE TABLE IF NOT EXISTS summoner_league (
        league_id VARCHAR,
        summoner_id VARCHAR,
        tier VARCHAR,
        rank VARCHAR,
        league_points INTEGER,
        veteran BOOLEAN,
        inactive BOOLEAN,
        fresh_blood BOOLEAN,
        hot_streak BOOLEAN,
        PRIMARY KEY (league_id, summoner_id),
        
        CONSTRAINT fk_summoner
            FOREIGN KEY (summoner_id)
            REFERENCES summoner(summoner_id)
            ON DELETE CASCADE
    )""")
    create_table(conn, """
    CREATE TABLE IF NOT EXISTS summoner_match (
        match_id VARCHAR,
        summoner_id VARCHAR,
        team_id INTEGER,
        end_of_game_result BOOLEAN,
        game_start_timestamp TIMESTAMP,
        game_end_timestamp TIMESTAMP,
        game_duration INTERVAL,
        champion_id INTEGER,
        champion_name VARCHAR,
        individual_position VARCHAR,
        team_position VARCHAR,
        summoner_spell1_id INTEGER,
        summoner_spell2_id INTEGER,
        summoner_spell1_casts INTEGER,
        summoner_spell2_casts INTEGER,
        kills INTEGER,
        deaths INTEGER,
        assists INTEGER,
        longest_time_living INTEGER,
        magic_damage_to_champion INTEGER,
        physical_damage_to_champion INTEGER,
        vision_score INTEGER,
        wards_placed INTEGER,
        wards_killed INTEGER,
        baron_kills INTEGER,
        dragon_kills INTEGER,
        voidmonster_kills INTEGER,
        gold_earned INTEGER,
        item0_id INTEGER,
        item1_id INTEGER,
        item2_id INTEGER,
        item3_id INTEGER,
        item4_id INTEGER,
        item5_id INTEGER,
        item6_id INTEGER,
        minion_cs INTEGER,
        jungle_cs INTEGER,
        game_ended_early_surrender BOOLEAN,
        game_ended_surrender BOOLEAN,
        kda FLOAT,
        total_ping_count INTEGER,
        primary_perk_style INTEGER,
        primary_perk1 INTEGER,
        primary_perk2 INTEGER,
        primary_perk3 INTEGER,
        sub_perk_style INTEGER,
        sub_perk1 INTEGER,
        sub_perk2 INTEGER,
        PRIMARY KEY (match_id, summoner_id),
        
        CONSTRAINT fk_summoner
            FOREIGN KEY (summoner_id)
            REFERENCES summoner(summoner_id)
            ON DELETE CASCADE
    )""")
    # for metabase
    try:
        create_database(conn, "metabase")
    except psycopg2.errors.DuplicateDatabase:
        print(f"[INFO] Metabase DB가 존재합니다.")


def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname=os.getenv("DB_NAME", "dbname"),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PSWD", "password")
    )
    conn.autocommit = True
    return conn


if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname=os.getenv("DB_NAME", "dbname"),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PSWD", "password")
    )
    conn.autocommit = True

    init_database(conn)
    ls_table(conn)
    
    conn.close()