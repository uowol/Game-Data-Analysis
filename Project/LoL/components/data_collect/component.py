import os
import random
import yaml
import pandas as pd
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.storage import duckdb
from modules.data_ingestion import riot_api
from components.formats import RequestDataCollect, ResponseDataCollect


class ComponentType(base.ComponentType):
    pass


class Component(base.Component):
    def init(self, **config):
        if config:
            self.config = ComponentType(**config)
        else:
            with open(os.path.join(Path(__file__).parent, "config.yaml"), "r") as fp:
                self.config = yaml.safe_load(fp)
                self.config = self.config if self.config is not None else {}

    def call(self, message: RequestDataCollect, *args, **kwargs) -> ResponseDataCollect:
        # --- download queue metadata ---
        global queue_metadata
        queue_metadata_url = "https://static.developer.riotgames.com/docs/lol/queues.json"
        queue_metadata_data = riot_api.get(queue_metadata_url)
        assert queue_metadata_data is not None, f"# [ERROR] Failed to download queue metadata from {queue_metadata_url}"
        queue_metadata_data += [
            {"queueId": 480, "map": "Summoner's Rift", "description": "Normal (Quickplay)", "notes": None},
        ]
        queue_metadata = pd.DataFrame(queue_metadata_data, columns=["queueId", "map", "description", "notes"])

        # --- make shards directory ---
        os.makedirs(message.shards_dir, exist_ok=True)
            
        # --- if resume is true, skip already collected data ---
        skip_summoners = []
        if message.resume:
            conn = duckdb.get_connection()
            metadata = pd.read_csv(f"{Path(message.shards_dir).parent}/metadata.csv")
            metadata = metadata.merge(
                conn.execute(
                    f"SELECT DISTINCT summoner_id, tier, rank AS division FROM '{message.shards_dir}/*.parquet'"
                ).fetchdf().groupby(['tier', 'division']).summoner_id.nunique().reset_index(name='cnt'),
                how='left', 
                on=['tier', 'division']
            )
            metadata.fillna({"cnt": 0}, inplace=True)
            resume_recipe = metadata[["tier", "division", "weight"]].copy()
            resume_recipe['sample_size'] = metadata.apply(lambda x: x.sample_size - x.cnt, axis=1)
            del metadata
            conn.close()
            print(f"# [INFO] resume recipe: \n{resume_recipe}")
        else:            
            # --- init metadata ---
            with open(f"{Path(message.shards_dir).parent}/metadata.csv", "w") as fp:
                fp.write("tier,division,weight,sample_size\n")

        # --- sampling summoners ---
        for recipe in message.recipe:
            if message.resume:
                sample_size = int(resume_recipe[resume_recipe.tier == recipe.tier].sample_size.values[0])
                if sample_size <= 0: continue
                weight = resume_recipe[resume_recipe.tier == recipe.tier].weight.values[0]
            else:
                sample_size = int(message.sample_size * recipe.ratio)
                if sample_size < 30:
                    sample_size = 30
                weight = recipe.ratio / sample_size
                # --- update metadata ---
                with open(f"{Path(message.shards_dir).parent}/metadata.csv", "a") as fp:
                    fp.write(f"{recipe.tier},{recipe.division if recipe.division else ''},{weight},{sample_size}\n")
            print(f"# [INFO] sampling summoners: {recipe.tier} {recipe.division if recipe.division else ""} {sample_size} ({weight})")
            
            # --- data collect --- TODO: league_data의 내용을 바탕으로 removed와 inserted를 구분하고 분기처리   
            page = 0
            n_loaded = 0

            while n_loaded < sample_size:
                page += 1
                league_data = self.get_league_data(
                    queue=message.queue,
                    tier=recipe.tier,
                    division=recipe.division,
                    page=page,
                )
                if len(league_data) == 0: 
                    print(f"# [INFO] no more data: {recipe.tier} {recipe.division if recipe.division else ''} {page}")
                    break
                random.shuffle(league_data)

                for summoner_league in league_data:
                    summoner_matchids = self.get_summoner_matchids_30d(message.date, summoner_league['puuid'])
                    records = []

                    for summoner_matchid in summoner_matchids:
                        try:
                            summoner_match_data = self.get_summoner_match_data(summoner_matchid, summoner_league)
                        except Exception as e:
                            # --- log error ---
                            with open(Path(message.shards_dir).parent / "error.log", "a") as fp:
                                print(f"\t{e}")
                                fp.write(f"{e}\n")
                            # --- pass error ---
                            continue
                        if summoner_match_data is None: break
                        summoner_match_data['tier'] = recipe.tier
                        summoner_match_data['rank'] = recipe.division
                        records.append(summoner_match_data)
                    
                    # --- save to parquet ---
                    if len(records) == 0: continue  # Fail Case
                    df = pd.DataFrame(records)
                    df.to_parquet(f"{message.shards_dir}/{summoner_league['summonerId']}.parquet", index=False)
                    print(f"# [INFO] ({n_loaded+1}/{sample_size}) insert sampled summoner: {summoner_league['summonerId']} ({len(records)})")
                    n_loaded += 1
                    if n_loaded >= sample_size: break

        return ResponseDataCollect(
            **message.model_dump(),
            result="success",
        )

    def get_league_data(self, queue: str, tier: str, division: str, page: int = 1):
        res = riot_api.get_league_by_queue_tier_division(queue=queue, tier=tier, division=division, page=page)
        assert res is not None, f"# [ERROR] Failed to download league data from {queue} {tier} {division}"
        if not division:
            return res['entries']
        return res

    def get_summoner_league_data(self, summoner_league: dict):
        return {
            "league_id": summoner_league["leagueId"],
            "summoner_id": summoner_league["summonerId"],
            "tier": summoner_league["tier"],
            "rank": summoner_league["rank"],
            "league_points": summoner_league["leaguePoints"],
            "veteran": summoner_league["veteran"],
            "inactive": summoner_league["inactive"],
            "fresh_blood": summoner_league["freshBlood"],
            "hot_streak": summoner_league["hotStreak"],
        }

    def get_summoner_matchids_30d(self, date: str, puuid: str):
        now = pd.to_datetime(date)
        res = riot_api.get_matchids_by_puuid(
            puuid=puuid, 
            startTime=int((now - timedelta(days=30) - datetime(1970, 1, 1)).total_seconds()), 
            count=100
        )
        while x:= riot_api.get_matchids_by_puuid(
            puuid=puuid, 
            startTime=int((now - timedelta(days=30) - datetime(1970, 1, 1)).total_seconds()), 
            start=len(res),
            count=100
        ):
            res += x
        assert res is not None, f"# [ERROR] Failed to download match ids from {puuid}"
        return res

    def get_summoner_match_data(self, matchid: str, summoner_league: dict):
        summoner_match = riot_api.get_match_by_matchid(matchid=matchid)
        assert summoner_match is not None, f"# [ERROR] Failed to download match data from {matchid}"

        # find summoner index
        summoner_index = -1
        for i, x in enumerate(
            summoner_match["metadata"]["participants"]
        ):
            if x == summoner_league["puuid"]:
                summoner_index = i
                break
        assert summoner_index != -1, f"Summoner ID {summoner_league['summonerId']} not found in match {matchid}"
        
        try:
            res = {
                "match_id": matchid,
                "summoner_id": summoner_league["summonerId"],
                "team_id": summoner_match["info"]["participants"][summoner_index]["teamId"],
                "end_of_game_result": summoner_match["info"]["endOfGameResult"] == "GameComplete",
                "game_start_timestamp": datetime(1970, 1, 1, 0, 0, 0)
                + timedelta(milliseconds=summoner_match["info"]["gameStartTimestamp"]),
                "game_end_timestamp": datetime(1970, 1, 1, 0, 0, 0)
                + timedelta(milliseconds=summoner_match["info"]["gameEndTimestamp"]),
                "game_duration": timedelta(milliseconds=summoner_match["info"]["gameDuration"]),
                # NOTE: API 문서에 gameDuration는 seconds로 되어있지만, 실제로는 milliseconds로 되어있음
                "game_mode": summoner_match["info"]["gameMode"],
                "queue_id": summoner_match["info"]["queueId"],
                "queue_description": queue_metadata.loc[
                    queue_metadata["queueId"] == summoner_match["info"]["queueId"], "description"
                ].values[0],
                "champion_id": summoner_match["info"]["participants"][summoner_index]["championId"],
                "champion_name": summoner_match["info"]["participants"][summoner_index]["championName"],
                "individual_position": summoner_match["info"]["participants"][summoner_index]["individualPosition"],
                "team_position": summoner_match["info"]["participants"][summoner_index]["teamPosition"],
                "summoner_spell1_id": summoner_match["info"]["participants"][summoner_index]["summoner1Id"],
                "summoner_spell2_id": summoner_match["info"]["participants"][summoner_index]["summoner2Id"],
                "summoner_spell1_casts": summoner_match["info"]["participants"][summoner_index]["summoner1Casts"],
                "summoner_spell2_casts": summoner_match["info"]["participants"][summoner_index]["summoner2Casts"],
                "kills": summoner_match["info"]["participants"][summoner_index]["kills"],
                "deaths": summoner_match["info"]["participants"][summoner_index]["deaths"],
                "assists": summoner_match["info"]["participants"][summoner_index]["assists"],
                "longest_time_living": summoner_match["info"]["participants"][summoner_index]["longestTimeSpentLiving"],
                "magic_damage_to_champion": summoner_match["info"]["participants"][summoner_index][
                    "magicDamageDealtToChampions"
                ],
                "physical_damage_to_champion": summoner_match["info"]["participants"][summoner_index][
                    "physicalDamageDealtToChampions"
                ],
                "vision_score": summoner_match["info"]["participants"][summoner_index]["visionScore"],
                "wards_placed": summoner_match["info"]["participants"][summoner_index]["wardsPlaced"],
                "wards_killed": summoner_match["info"]["participants"][summoner_index]["wardsKilled"],
                "baron_kills": summoner_match["info"]["participants"][summoner_index]["baronKills"],
                "dragon_kills": summoner_match["info"]["participants"][summoner_index]["dragonKills"],
                "voidmonster_kills": summoner_match["info"]["participants"][summoner_index]["challenges"][
                    "voidMonsterKill"
                ],
                "gold_earned": summoner_match["info"]["participants"][summoner_index]["goldEarned"],
                "item0_id": summoner_match["info"]["participants"][summoner_index]["item0"],
                "item1_id": summoner_match["info"]["participants"][summoner_index]["item1"],
                "item2_id": summoner_match["info"]["participants"][summoner_index]["item2"],
                "item3_id": summoner_match["info"]["participants"][summoner_index]["item3"],
                "item4_id": summoner_match["info"]["participants"][summoner_index]["item4"],
                "item5_id": summoner_match["info"]["participants"][summoner_index]["item5"],
                "item6_id": summoner_match["info"]["participants"][summoner_index]["item6"],
                "minion_cs": summoner_match["info"]["participants"][summoner_index]["totalMinionsKilled"],
                "jungle_cs": summoner_match["info"]["participants"][summoner_index]["neutralMinionsKilled"],
                "game_ended_early_surrender": summoner_match["info"]["participants"][summoner_index][
                    "gameEndedInEarlySurrender"
                ],
                "game_ended_surrender": summoner_match["info"]["participants"][summoner_index]["gameEndedInSurrender"],
                "kda": summoner_match["info"]["participants"][summoner_index]["challenges"]["kda"],
                "total_ping_count": sum(
                    [v for k, v in summoner_match["info"]["participants"][summoner_index].items() if "Pings" in k]
                ),
                "primary_perk_style": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][0]["style"],
                "primary_perk1": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][0]["selections"][
                    0
                ]["perk"],
                "primary_perk2": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][0]["selections"][
                    1
                ]["perk"],
                "primary_perk3": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][0]["selections"][
                    2
                ]["perk"],
                "sub_perk_style": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][1]["style"],
                "sub_perk1": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][1]["selections"][0][
                    "perk"
                ],
                "sub_perk2": summoner_match["info"]["participants"][summoner_index]["perks"]["styles"][1]["selections"][1][
                    "perk"
                ],
            }
        except Exception as e:
            print(f"# [ERROR] {e}")
            print(summoner_match)
            return None
        return res
