# API를 통한 데이터 수집
import aiohttp
import asyncio
import aiofiles
import os
import json 
import time
from dotenv import load_dotenv
from collections import deque
from modules.constants import RATE_LIMIT_1S, RATE_LIMIT_2M

load_dotenv()

RIOT_API_KEY = os.getenv("RIOT_API_KEY")
OUTPUT_PATH = "/home/leagueoflegends/data"
G_VISITED = set(os.listdir(OUTPUT_PATH + "/match")) if os.path.exists(OUTPUT_PATH + "/match") else set()
SESSION = None

semaphore = asyncio.Semaphore(RATE_LIMIT_1S)
request_times = deque(maxlen=RATE_LIMIT_2M)


async def rate_limit():
    async with semaphore:
        now = time.time()
        while request_times and now - request_times[0] > 120:
            request_times.popleft()
        
        if len(request_times) >= RATE_LIMIT_2M:
            wait_time = 120 - (now - request_times[0])
            print(f"[INFO] rate limit about 2min! wait {wait_time} sec...")
            await asyncio.sleep(wait_time)
        
        request_times.append(time.time())
        
        await asyncio.sleep(1/RATE_LIMIT_1S)

async def get(url: str):
    await rate_limit()  # 요청 속도 조절
    async with SESSION.get(url) as response:

        if response.status == 429:
            retry_after = response.headers.get('Retry-After', 60)
            print(f"[INFO] 429 error, wait {retry_after} sec...")
            await asyncio.sleep(int(retry_after))
            return await get(url)

        while response.status != 200:
            print(f"[ERROR] {response.status} error, after 30 seconds, retrying")
            await asyncio.sleep(30)
            response = await SESSION.get(url)

        return await response.json()

async def friend_of_a_friend(puuid:str, name: str = None, tag: str = None, verbose: bool = False):
    if puuid is None:
        assert name is not None and tag is not None
        puuid = (await get_account_by_name_n_tag(name, tag))['puuid']

    if puuid in G_VISITED:
        return

    if verbose:
        print(f"[INFO] now visiting {puuid}")
    
    G_VISITED.add(puuid)
    matchids = await export_match_by_puuid(puuid, OUTPUT_PATH + "/match")
    tasks = []
    
    for matchid in matchids:
        match = await get_match_by_matchid(matchid)
    
        for participant in match['metadata']['participants']:
            if participant not in G_VISITED:
                tasks.append(friend_of_a_friend(participant, verbose=verbose))
    
    if tasks:
        # 병렬적으로 실행할 경우 export 이전에 다음 task로 넘어가는 문제가 있음
        for task in tasks:
            await task
        # await asyncio.gather(*tasks)
        
            
async def get_account_by_name_n_tag(name: str, tag: str):
    url = f"https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{name}/{tag}?api_key=" + RIOT_API_KEY
    return await get(url)

async def get_matchids_by_puuid(puuid: str):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?api_key=" + RIOT_API_KEY
    return await get(url)

async def get_match_by_matchid(matchid: str):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}?api_key=" + RIOT_API_KEY
    return await get(url)

async def get_matchtimeline_by_matchid(matchid: str):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}/timeline?api_key=" + RIOT_API_KEY
    return await get(url)

async def export_match_by_matchid(matchid: str, output_dir: str, verbose: bool = False):
    if verbose:
        print(f"[INFO] now exporting match data of {matchid}")
        
    output_dir = os.path.join(output_dir, matchid)
    os.makedirs(output_dir, exist_ok=True)
    
    match = await get_match_by_matchid(matchid)
    timeline = await get_matchtimeline_by_matchid(matchid)
    
    async def write_json(filename, data):
        async with aiofiles.open(os.path.join(output_dir, filename), "w") as f:
            await f.write(json.dumps(data, indent=4, ensure_ascii=False))
            await f.flush()
        if verbose:
            print(f"[INFO] File saved: {filename}")

    asyncio.gather(
        write_json(matchid + ".json", match),
        write_json(matchid + "_timeline.json", timeline)
    )
    
    return match, timeline

async def export_match_by_puuid(puuid: str, output_dir: str, verbose: bool = False):
    if verbose:
        print(f"[INFO] now exporting match data of {puuid}")

    output_dir = os.path.join(output_dir, puuid)
    os.makedirs(output_dir, exist_ok=True)

    matchids = await get_matchids_by_puuid(puuid)
    tasks = [export_match_by_matchid(matchid, output_dir, verbose=True) for matchid in matchids]

    await asyncio.gather(*tasks)
    return matchids    


async def main():
    global SESSION
    async with aiohttp.ClientSession() as session:
        SESSION = session
        print("[DEBUG] SESSION is initialized:", SESSION)
        await friend_of_a_friend(puuid=None, name="야식은치킨이지", tag="KR1", verbose=True)
        print("[DEBUG] SESSION is closed:", SESSION)        

if __name__ == "__main__":
    asyncio.run(main())