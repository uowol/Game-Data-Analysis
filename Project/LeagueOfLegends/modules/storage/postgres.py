import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="db",
    user="kcw",
    password="sk1346"
)
conn.autocommit = True

def ls_table(conn):
    cur = conn.cursor()
    cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cur.fetchall()
    print(tables)
    cur.close()
    

def create_table(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    cur.close()

create_table(conn,"""
CREATE TABLE IF NOT EXISTS summoner (
    summoner_id VARCHAR PRIMARY KEY,
    puuid VARCHAR,
    game_name VARCHAR,
    game_tage VARCHAR
);""")
create_table(conn, """
CREATE TABLE IF NOT EXISTS summoner_league (
    league_id VARCHAR,
    summoner_id VARCHAR,
    tier VARCHAR,
    rank VARCHAR,
    league_points INTEGER,
    vetran BOOLEAN,
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
CREATE TABLE champion(
    champion_id INTEGER PRIMARY KEY,
    champion_name VARCHAR
)""")
create_table(conn, """
CREATE TABLE IF NOT EXISTS summoner_match (
    match_id VARCHAR,
    summoner_id VARCHAR,
    team_id INTEGER,
    end_of_game_result BOOLEAN,
    game_start_timestamp TIMESTAMP,
    game_end_timestamp TIMESTAMP,
    game_duration INTEGER,
    champion_id INTEGER,
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
    riftherald_kills INTEGER,
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
    primart_perk1 INTEGER,
    primary_perk2 INTEGER,
    primary_perk3 INTEGER,
    sub_perk_style INTEGER,
    sub_perk1 INTEGER,
    sub_perk2 INTEGER,
    PRIMARY KEY (match_id, summoner_id),
    
    CONSTRAINT fk_summoner
        FOREIGN KEY (summoner_id)
        REFERENCES summoner(summoner_id)
        ON DELETE CASCADE,
        
    CONSTRAINT fk_champion
        FOREIGN KEY (champion_id)
        REFERENCES champion(champion_id)
        ON DELETE CASCADE
)""")
ls_table(conn)
conn.close()