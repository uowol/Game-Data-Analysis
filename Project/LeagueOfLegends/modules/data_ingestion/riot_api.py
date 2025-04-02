import requests
import time
import os
import json
from dotenv import load_dotenv
load_dotenv()


def get(url: str):
    now = time.time()
    response = requests.get(url)
    if response.status_code == 429:
        retry_after = response.headers.get('Retry-After', 60)
        print(f"[ERROR:429] Too many requests, wait {retry_after} sec...")
        time.sleep(int(retry_after)+1)
        return get(url)
    while response.status_code != 200:        
        print(f"[ERROR:{response.status_code}] Error, after 30 seconds, retrying")
        time.sleep(30)
        response = requests.get(url)
    return response.json()

def get_account_by_puuid(puuid: str):
    url = f"https://asia.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}?api_key=" + os.getenv("RIOT_KEY")
    return get(url)

def get_account_by_name_n_tag(name: str, tag: str):
    url = f"https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{name}/{tag}?api_key=" + os.getenv("RIOT_KEY")
    return get(url)

def get_matchids_by_puuid(puuid: str, *, startTime: int = 0, start: int = 0, count: int = 20):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={startTime}&start={start}&count={count}&api_key=" + os.getenv("RIOT_KEY")
    return get(url)

def get_match_by_matchid(matchid: str):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}?api_key=" + os.getenv("RIOT_KEY")
    return get(url)

def get_matchtimeline_by_matchid(matchid: str):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}/timeline?api_key=" + os.getenv("RIOT_KEY")
    return get(url)

def get_league_by_queue_tier_division(queue: str, tier: str, division: str):
    url = f"https://kr.api.riotgames.com/lol/league/v4/entries/{queue}/{tier}/{division}?page={1}&api_key=" + os.getenv("RIOT_KEY")
    return get(url)

def export_json(json_data, output_path):
    with open(output_path, 'w') as f:
        json.dump(json_data, f, indent=4)


if __name__ == "__main__":
    x = get_league_by_queue_tier_division("RANKED_SOLO_5x5", "DIAMOND", "I")
    export_json(x, "modules/data_ingestion/output/league.json")
    y = get_account_by_puuid(x[0]['puuid'])
    export_json(y, "modules/data_ingestion/output/account.json")
    z = get_matchids_by_puuid(x[0]['puuid'])
    export_json(z, "modules/data_ingestion/output/matchids.json")
    k = get_match_by_matchid(z[0])
    export_json(k, "modules/data_ingestion/output/match.json")
    
    # print(z.keys())
    # print(z['info'].keys())
    # participants = z['metadata']['participants']
    # y = get_matchlist_by_puuid(participants[0])
    # print(y)