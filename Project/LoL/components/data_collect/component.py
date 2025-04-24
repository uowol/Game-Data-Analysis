import yaml
import psycopg2
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.data_ingestion import riot_api
from modules.storage import postgres
from components.formats import RequestDataCollect, ResponseDataCollect, ResponseMessage

class ComponentType(base.ComponentType):
    pass    

class Component(base.Component):
    def init(self, **config):
        if config:
            self.config = ComponentType(**config)
        else:
            with open("components/data_collect/config.yaml", 'r') as fp:
                self.config = yaml.safe_load(fp)
                self.config = self.config if self.config is not None else {}


    def call(self, message: RequestDataCollect, *args, **kwargs) -> ResponseDataCollect:
        return self.call_postgres(message, *args, **kwargs)

    def call_postgres(
        self, 
        message: RequestDataCollect, 
        *,
        upstream_events: List[ResponseMessage] = []
    ) -> ResponseDataCollect:
        conn = postgres.get_connection()    # NOTE: with 문법을 사용할 경우 의도와 달리 트랜잭션으로 간주될 수 있음
        
        # init database
        for table in self.config['query']['create']['table']:
            tables = postgres.ls_table(conn)
            if table not in tables:
                print(f"# [INFO] create table: {table}")
                postgres.excute_query(conn, self.config['query']['create']['table'][table])
        try:
            postgres.excute_query(conn, self.config['query']['create']['db']['metabase'])
        except psycopg2.errors.DuplicateDatabase:
            print(f"[INFO] Metabase DB가 존재합니다.")
        
        # insert data
        league_data = riot_api.get_league_by_queue_tier_division(
            queue=message.queue,
            tier=message.tier,
            division=message.division
        )
        for summoner_league in league_data:
            # table: summoner_league
            league_id = summoner_league['leagueId']
            summoner_id = summoner_league['summonerId']
            tier = summoner_league['tier']
            rank = summoner_league['rank']
            league_points = summoner_league['leaguePoints']
            veteran = summoner_league['veteran']
            inactive = summoner_league['inactive']
            fresh_blood = summoner_league['freshBlood']
            hot_streak = summoner_league['hotStreak']
            
            # table: summoner
            summoner = riot_api.get_account_by_puuid(summoner_league['puuid'])
            puuid = summoner['puuid']
            game_name = summoner['gameName']
            game_tag = summoner['tagLine']
            
            # insert: summoner
            print(f"# [INFO] insert summoner: {summoner_id}")
            query = postgres.create_insert_query(
                table_name='summoner',
                columns=['summoner_id', 'puuid', 'game_name', 'game_tag'],
                primary_keys=['summoner_id']
            )
            postgres.excute_query(conn, query, data=[summoner_id, puuid, game_name, game_tag])
            
            # insert: summoner_league
            print(f"# [INFO] insert summoner_league: {(league_id, summoner_id)}")
            query = postgres.create_insert_query(
                table_name='summoner_league',
                columns=['league_id', 'summoner_id', 'tier', 'rank', 'league_points', 'veteran', 'inactive', 'fresh_blood', 'hot_streak'],
                primary_keys=['league_id', 'summoner_id']
            )
            postgres.excute_query(conn, query, data=[league_id, summoner_id, tier, rank, league_points, veteran, inactive, fresh_blood, hot_streak])
            
            # TODO: 여기 로직이 조금 비효율적일 수 있음. 확인 필요
            summoner_matchids = riot_api.get_matchids_by_puuid(summoner['puuid'])
            for summoner_matchid in summoner_matchids:
                summoner_match = riot_api.get_match_by_matchid(summoner_matchid)
                match_id = summoner_match['metadata']['matchId']    # NOTE: 여기서 match_id가 이미 적재되어 있다면 패스하는 로직이 필요

                # NOTE: 임시, 현재 summoner_id의 summoner_index를 찾아 사용
                summoner_index = -1
                for i, x in enumerate(summoner_match['metadata']['participants']):   # NOTE: 리그는 맞는데, 최근 게임으로 칼바람 등이 들어올 수 있음
                    if x == puuid:
                        summoner_index = i
                        break
                assert summoner_index != -1, f"Summoner ID {summoner_id} not found in match {match_id}"
                # for summoner_index in range(10):
                team_id = summoner_match['info']['participants'][summoner_index]['teamId']
                # summoner_id = summoner_match['metadata']['participants'][summoner_index]    # NOTE: 여기서 summoner_id를 계속 바꾸고 있음. 아래에서 사용하지 말 것.
                end_of_game_result = summoner_match['info']['endOfGameResult'] == 'GameComplete'
                game_start_timestamp = datetime(1970, 1, 1, 0, 0, 0) + timedelta(milliseconds=summoner_match['info']['gameStartTimestamp'])
                game_end_timestamp = datetime(1970, 1, 1, 0, 0, 0) + timedelta(milliseconds=summoner_match['info']['gameEndTimestamp'])
                game_duration = timedelta(seconds=summoner_match['info']['gameDuration'])
                champion_id = summoner_match['info']['participants'][summoner_index]['championId']
                champion_name = summoner_match['info']['participants'][summoner_index]['championName']
                individual_position = summoner_match['info']['participants'][summoner_index]['individualPosition']
                team_position = summoner_match['info']['participants'][summoner_index]['teamPosition']
                summoner_spell1_id = summoner_match['info']['participants'][summoner_index]['summoner1Id']
                summoner_spell2_id = summoner_match['info']['participants'][summoner_index]['summoner2Id']
                summoner_spell1_casts = summoner_match['info']['participants'][summoner_index]['summoner1Casts']
                summoner_spell2_casts = summoner_match['info']['participants'][summoner_index]['summoner2Casts']
                kills = summoner_match['info']['participants'][summoner_index]['kills']
                deaths = summoner_match['info']['participants'][summoner_index]['deaths']
                assists = summoner_match['info']['participants'][summoner_index]['assists']
                longest_time_living = summoner_match['info']['participants'][summoner_index]['longestTimeSpentLiving']
                magic_damage_to_champion = summoner_match['info']['participants'][summoner_index]['magicDamageDealtToChampions']
                physical_damage_to_champion = summoner_match['info']['participants'][summoner_index]['physicalDamageDealtToChampions']
                vision_score = summoner_match['info']['participants'][summoner_index]['visionScore']
                wards_placed = summoner_match['info']['participants'][summoner_index]['wardsPlaced']
                wards_killed = summoner_match['info']['participants'][summoner_index]['wardsKilled']
                baron_kills = summoner_match['info']['participants'][summoner_index]['baronKills']
                dragon_kills = summoner_match['info']['participants'][summoner_index]['dragonKills']
                # riftherald_kills = summoner_match['info']['participants'][summoner_index]['riftHeraldKills']
                voidmonster_kills = summoner_match['info']['participants'][summoner_index]['challenges']['voidMonsterKill']
                gold_earned = summoner_match['info']['participants'][summoner_index]['goldEarned']
                item0_id = summoner_match['info']['participants'][summoner_index]['item0']
                item1_id = summoner_match['info']['participants'][summoner_index]['item1']
                item2_id = summoner_match['info']['participants'][summoner_index]['item2']
                item3_id = summoner_match['info']['participants'][summoner_index]['item3']
                item4_id = summoner_match['info']['participants'][summoner_index]['item4']
                item5_id = summoner_match['info']['participants'][summoner_index]['item5']
                item6_id = summoner_match['info']['participants'][summoner_index]['item6']
                minion_cs = summoner_match['info']['participants'][summoner_index]['totalMinionsKilled']
                jungle_cs = summoner_match['info']['participants'][summoner_index]['neutralMinionsKilled']
                game_ended_early_surrender = summoner_match['info']['participants'][summoner_index]['gameEndedInEarlySurrender']
                game_ended_surrender = summoner_match['info']['participants'][summoner_index]['gameEndedInSurrender']
                kda = summoner_match['info']['participants'][summoner_index]['challenges']['kda']
                total_ping_count = sum([v for k, v in summoner_match['info']['participants'][summoner_index].items() if 'Pings' in k])
                primary_perk_style = summoner_match['info']['participants'][summoner_index]['perks']['styles'][0]['style']
                primary_perk1 = summoner_match['info']['participants'][summoner_index]['perks']['styles'][0]['selections'][0]['perk']
                primary_perk2 = summoner_match['info']['participants'][summoner_index]['perks']['styles'][0]['selections'][1]['perk']
                primary_perk3 = summoner_match['info']['participants'][summoner_index]['perks']['styles'][0]['selections'][2]['perk']
                sub_perk_style = summoner_match['info']['participants'][summoner_index]['perks']['styles'][1]['style']
                sub_perk1 = summoner_match['info']['participants'][summoner_index]['perks']['styles'][1]['selections'][0]['perk']
                sub_perk2 = summoner_match['info']['participants'][summoner_index]['perks']['styles'][1]['selections'][1]['perk']
                
                # insert: summoner_match
                print(f"# [INFO] insert summoner_match: {(match_id, summoner_id)}")
                query = postgres.create_insert_query(
                    table_name='summoner_match',
                    columns=['match_id', 'summoner_id', 'team_id', 'end_of_game_result', 'game_start_timestamp', 'game_end_timestamp', 'game_duration', 'champion_id', 'champion_name', 'individual_position', 'team_position', 'summoner_spell1_id', 'summoner_spell2_id', 'summoner_spell1_casts', 'summoner_spell2_casts', 'kills', 'deaths', 'assists', 'longest_time_living', 'magic_damage_to_champion', 'physical_damage_to_champion', 'vision_score', 'wards_placed', 'wards_killed', 'baron_kills', 'dragon_kills', 'voidmonster_kills', 'gold_earned', 'item0_id', 'item1_id', 'item2_id', 'item3_id', 'item4_id', 'item5_id','item6_id','minion_cs','jungle_cs','game_ended_early_surrender','game_ended_surrender','kda','total_ping_count','primary_perk_style','primary_perk1','primary_perk2','primary_perk3','sub_perk_style','sub_perk1','sub_perk2'],
                    primary_keys=['match_id', 'summoner_id']
                )
                postgres.excute_query(
                    conn, 
                    query, 
                    data=[match_id, summoner_id, team_id, end_of_game_result, game_start_timestamp, game_end_timestamp, game_duration, champion_id, champion_name, individual_position, team_position, summoner_spell1_id, summoner_spell2_id, summoner_spell1_casts, summoner_spell2_casts, kills, deaths, assists, longest_time_living, magic_damage_to_champion, physical_damage_to_champion, vision_score, wards_placed, wards_killed, baron_kills, dragon_kills, voidmonster_kills, gold_earned, item0_id, item1_id, item2_id, item3_id, item4_id, item5_id,item6_id,minion_cs,jungle_cs,game_ended_early_surrender,game_ended_surrender,kda,total_ping_count,primary_perk_style,primary_perk1,primary_perk2,primary_perk3,sub_perk_style,sub_perk1,sub_perk2]
                )
            
        conn.close()

        return ResponseDataCollect(
            queue=message.queue,
            tier=message.tier,
            division=message.division,
            output_dir=message.output_dir
        )