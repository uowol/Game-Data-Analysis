import os
import yaml
import pandas as pd
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.data_ingestion import riot_api
from modules.storage import duckdb
from components.formats import RequestDataCollect, ResponseDataCollect, ResponseMessage


class ComponentType(base.ComponentType):
    pass


class Component(base.Component):
    def init(self, **config):
        if config:
            self.config = ComponentType(**config)
        else:
            with open("components/data_collect/config.yaml", "r") as fp:
                self.config = yaml.safe_load(fp)
                self.config = self.config if self.config is not None else {}

    def call(self, message: RequestDataCollect, *args, **kwargs) -> ResponseDataCollect:
        # --- get duckdb connection ---
        os.makedirs("../../metabase/data", exist_ok=True)
        conn = duckdb.get_connection("../../metabase/data/duckdb.db")

        # --- init database ---
        for table in self.config["query"]["dwh"]["create"]["table"]:
            tables = duckdb.ls_table(conn)
            if table not in tables:
                print(f"# [INFO] create table: {table}")
                duckdb.excute_query(conn, self.config["query"]["dwh"]["create"]["table"][table])
        
        # --- insert data ---
        league_data = self.get_league_data(message.queue, message.tier, message.division)
        for summoner_league in league_data:
            # table: summoner
            summoner_data = self.get_summoner_data(summoner_league)

            # table: summoner_league
            summoner_league_data = self.get_summoner_league_data(summoner_league)

            # insert: summoner
            print(f"# [INFO] insert summoner: {summoner_data['summoner_id']}")
            query = duckdb.create_insert_query(
                table_name="summoner",
                columns=summoner_data.keys(),
            )
            duckdb.excute_query(conn, query, params=list(summoner_data.values()))

            # insert: summoner_league
            print(
                f"# [INFO] insert summoner_league: {(summoner_league_data['league_id'], summoner_league_data['summoner_id'])}"
            )
            query = duckdb.create_insert_query(
                table_name="summoner_league",
                columns=summoner_league_data.keys(),
            )
            duckdb.excute_query(
                conn,
                query,
                params=list(summoner_league_data.values()),
            )

            summoner_matchids = self.get_summoner_matchids(summoner_data["puuid"])
            for summoner_matchid in summoner_matchids:
                summoner_match_data = self.get_summoner_match_data(summoner_matchid, summoner_data)

                # insert: summoner_match
                print(
                    f"# [INFO] insert summoner_match: {(summoner_match_data['match_id'], summoner_match_data['summoner_id'])}"
                )
                query = duckdb.create_insert_query(
                    table_name="summoner_match",
                    columns=summoner_match_data.keys(),
                )
                duckdb.excute_query(
                    conn,
                    query,
                    params=list(summoner_match_data.values()),
                )

            # --- save recent data ---
            output_dir = os.path.join(message.output_dir, "recent-1-summoner")
            os.makedirs(output_dir, exist_ok=True)

            for table_name in self.config["query"]["dwh"]["create"]["table"]:
                query = f"SELECT * FROM {table_name} WHERE summoner_id = ?"
                if self.config["setting"]["save_format"] == "csv":
                    duckdb.excute_query(
                        conn,
                        f"""
                        COPY ({query}) TO '{output_dir}/{table_name}.csv' (DELIMITER ',', HEADER TRUE);
                        """,
                        params=[summoner_data["summoner_id"]],
                    )
                if self.config["setting"]["save_format"] == "parquet":
                    duckdb.excute_query(
                        conn,
                        f"""
                        COPY ({query}) TO '{output_dir}/{table_name}.parquet' (FORMAT PARQUET);
                        """,
                        params=[summoner_data["summoner_id"]],
                    )

        # --- close duckdb connection ---
        conn.close()
        
        # --- init metabase ---
        # duckdb.docker_build_metabase()    # if need to build metabase image
        # duckdb.docker_run_metabase()        # if it already started, it will be passed


        return ResponseDataCollect(
            queue=message.queue,
            tier=message.tier,
            division=message.division,
            output_dir=message.output_dir,
        )

    def get_league_data(self, queue: str, tier: str, division: str):
        return riot_api.get_league_by_queue_tier_division(queue=queue, tier=tier, division=division)

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

    def get_summoner_data(self, summoner_league: dict):
        account = riot_api.get_account_by_puuid(summoner_league["puuid"])
        summoner = riot_api.get_summoner_by_puuid(summoner_league["puuid"])
        return {
            "summoner_id": summoner["id"],
            "puuid": account["puuid"],
            "game_name": account["gameName"],
            "game_tag": account["tagLine"],
        }

    def get_summoner_matchids(self, puuid: str):
        return riot_api.get_matchids_by_puuid(puuid=puuid)

    def get_summoner_match_data(self, matchid: str, summoner_data: dict):
        # download metadata
        queue_metadata_url = "https://static.developer.riotgames.com/docs/lol/queues.json"
        queue_metadata_data = riot_api.get(queue_metadata_url)
        queue_metadata = pd.DataFrame(queue_metadata_data, columns=["queueId", "map", "description", "notes"])
        
        summoner_match = riot_api.get_match_by_matchid(matchid=matchid)
        match_id = summoner_match["metadata"]["matchId"]
        # NOTE: 여기서 match_id가 이미 적재되어 있다면 패스하는 로직이 필요

        # NOTE: 임시, 현재 summoner_id의 summoner_index를 찾아 사용
        summoner_index = -1
        for i, x in enumerate(
            summoner_match["metadata"]["participants"]
        ):
            if x == summoner_data["puuid"]:
                summoner_index = i
                break
        assert summoner_index != -1, f"Summoner ID {summoner_data['summoner_id']} not found in match {match_id}"

        return {
            "match_id": match_id,
            "summoner_id": summoner_data["summoner_id"],
            "team_id": summoner_match["info"]["participants"][summoner_index]["teamId"],
            "end_of_game_result": summoner_match["info"]["endOfGameResult"] == "GameComplete",
            "game_start_timestamp": datetime(1970, 1, 1, 0, 0, 0)
            + timedelta(milliseconds=summoner_match["info"]["gameStartTimestamp"]),
            "game_end_timestamp": datetime(1970, 1, 1, 0, 0, 0)
            + timedelta(milliseconds=summoner_match["info"]["gameEndTimestamp"]),
            "game_duration": timedelta(seconds=summoner_match["info"]["gameDuration"]),
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
