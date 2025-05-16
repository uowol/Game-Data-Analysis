import os
import yaml
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.storage import duckdb
from components.formats import RequestDataAnalyze, ResponseDataAnalyze


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

    def call(self, message: RequestDataAnalyze, *args, **kwargs) -> ResponseDataAnalyze:
        # --- load metadata(tier, weight) ---
        weight_df = pd.read_csv(Path(message.duckdb_filepath).parent / "metadata.csv")
        weight_df["key"] = list(zip(weight_df["tier"], weight_df["division"].fillna("")))
        weight_map = dict(zip(weight_df["key"], weight_df["weight"]))

        # --- load raw_data as dataframe ---
        conn = duckdb.get_connection(message.duckdb_filepath)

        df = conn.execute(
            "SELECT * FROM raw_summoner_game_logs where game_start_timestamp >= DATE '2025-04-14';"
        ).fetchdf()
        df["timestamp"] = df["game_start_timestamp"].apply(lambda x: x.date())
        df["division"] = df["rank"].fillna("")
        df["w"] = list(df.apply(lambda r: weight_map.get((r["tier"], r["division"]), 1.0), axis=1))
        print(f"[INFO] Load {len(df)} records: {df.summoner_id.nunique()} summoners.")

        conn.close()

        # --- load report connection ---
        conn = duckdb.get_connection(message.report_filepath)

        # --- 1.1 D-n Retention Rate ---
        today = df.timestamp.max()
        target = today - timedelta(days=30)

        target_mask = df["timestamp"] == target
        target_users_w = df[target_mask].groupby("summoner_id")["w"].mean()
        target_total_w = len(target_users_w)

        res = []
        for d in [1, 3, 7, 30]:
            d_date = target + timedelta(days=d)
            comeback_mask = df["timestamp"] == d_date
            comeback_users_w = df[comeback_mask].groupby("summoner_id")["w"].mean()

            inter = target_users_w.index.intersection(comeback_users_w.index)
            retained_w = len(target_users_w.loc[inter])

            d_retention = retained_w / target_total_w if target_total_w else 0
            res.append({"day": d, "retention": d_retention})
            print(f"[INFO] D-{d} Weighted Retention = {d_retention:.4f}")
        retention_df = pd.DataFrame(res)
        conn.execute("CREATE OR REPLACE TABLE retention AS SELECT * FROM retention_df;")
        del retention_df

        # --- 1.2 Average / Median Session Length ---
        g = (
            df.groupby(["game_mode", "timestamp", "summoner_id"])
            .agg(session_length=("game_duration", "sum"), w=("w", "mean"))
            .reset_index()
        )

        def w_avg(x: pd.DataFrame) -> float:
            return np.average(x["session_length"].dt.total_seconds() / 60, weights=x["w"])

        avg_len = (
            g.groupby(["game_mode", "timestamp"]).apply(w_avg, include_groups=False).reset_index(name="avg_minutes")
        )

        def w_median(tbl: pd.DataFrame) -> float:
            tbl = tbl.assign(m=tbl["session_length"].dt.total_seconds() / 60)
            tbl = tbl.sort_values("m")
            cum_w = tbl["w"].cumsum()
            cutoff = tbl["w"].sum() / 2
            return tbl.loc[cum_w >= cutoff, "m"].iloc[0]

        median_len = (
            g.groupby(["game_mode", "timestamp"])
            .apply(w_median, include_groups=False)
            .reset_index(name="median_minutes")
        )

        conn.execute("CREATE OR REPLACE TABLE avg_session_length AS SELECT * FROM avg_len;")
        conn.execute("CREATE OR REPLACE TABLE median_session_length AS SELECT * FROM median_len;")
        del avg_len, median_len

        # --- 1.3 Sessions per User per Day ---
        per_day = (
            df.groupby(["game_mode", "timestamp", "summoner_id"])
            .agg(sess=("summoner_id", "size"), w=("w", "mean"))
            .reset_index()
        )

        def w_avg_sess(x: pd.DataFrame) -> float:
            return np.average(x["sess"], weights=x["w"])

        tmp = (
            per_day.groupby(["game_mode", "timestamp"])
            .apply(w_avg_sess, include_groups=False)
            .reset_index(name="result")
        )

        conn.execute("CREATE OR REPLACE TABLE sessions_per_user_per_day AS SELECT * FROM tmp;")
        del tmp

        # --- 1.4 Churn Rate ---
        cohort_date = today - timedelta(days=30)
        cohort_users = df[df["timestamp"] == cohort_date].groupby("summoner_id")["w"].mean()
        cohort_w = cohort_users.sum()

        res = []
        for d in [3, 7, 14, 30]:
            cutoff = cohort_date + timedelta(days=d)
            returned = (
                df[(df["timestamp"] > cohort_date) & (df["timestamp"] <= cutoff)].groupby("summoner_id")["w"].mean()
            )
            inter = cohort_users.index.intersection(returned.index)
            retained_w = cohort_users.loc[inter].sum()
            churn_rate = 1 - retained_w / cohort_w if cohort_w else 0
            res.append({"day": d, "churn_rate": churn_rate})
            print(f"[INFO] D-{d} Weighted Churn = {churn_rate:.4f}")

        churn_df = pd.DataFrame(res)
        conn.execute("CREATE OR REPLACE TABLE churn_rate AS SELECT * FROM churn_df;")
        del churn_df

        # --- 2.1 Access Rate to New Content ---
        target_modes = ["CHERRY", "SWIFTPLAY"]
        target_champ = "Mel"

        res_modes = []
        for mode in target_modes:
            for d in [0, 14, 30]:
                day = today - timedelta(days=d)
                daily = df[df["timestamp"] == day]
                tot_w = daily.groupby("summoner_id")["w"].mean().sum()
                new_w = daily[daily["game_mode"] == mode].groupby("summoner_id")["w"].mean().sum()
                rate = new_w / tot_w if tot_w else 0
                res_modes.append({"date": day, "mode": mode, "rate": rate})
                print(f"[INFO] {day} Access '{mode}': {rate:.4f}")

        res_champ = []
        for d in [0, 14, 30]:
            day = today - timedelta(days=d)
            daily = df[df["timestamp"] == day]
            tot_w = daily.groupby("summoner_id")["w"].mean().sum()
            new_w = daily[daily["champion_name"] == target_champ].groupby("summoner_id")["w"].mean().sum()
            rate = new_w / tot_w if tot_w else 0
            res_champ.append({"date": day, "champion": target_champ, "rate": rate})
            print(f"[INFO] {day} Access '{target_champ}': {rate:.4f}")

        access_rate_new_modes_df = pd.DataFrame(res_modes)
        access_rate_new_champ_df = pd.DataFrame(res_champ)
        conn.execute("CREATE OR REPLACE TABLE access_rate_new_modes AS SELECT * FROM access_rate_new_modes_df;")
        conn.execute("CREATE OR REPLACE TABLE access_rate_new_champ AS SELECT * FROM access_rate_new_champ_df;")
        del access_rate_new_modes_df, access_rate_new_champ_df

        # --- 2.2 Average / Median Play Count per User to New Content ---
        res_modes = []
        for mode in target_modes:
            pc = df[df["game_mode"] == mode].groupby("summoner_id").agg(plays=("game_mode", "size"), w=("w", "mean"))
            # NOTE: 0회 유저 포함하기
            all_w = df.groupby("summoner_id")["w"].mean()
            pc = pc.reindex(all_w.index, fill_value=0)
            pc["w"] = all_w

            avg_play = np.average(pc["plays"], weights=pc["w"])
            pc_sorted = pc.sort_values("plays")
            cum_w = pc_sorted["w"].cumsum()
            median_play = pc_sorted.loc[cum_w >= pc_sorted["w"].sum() / 2, "plays"].iloc[0]
            res_modes.append({"mode": mode, "avg_play_per_user": avg_play, "median_play_per_user": median_play})
            print(f"[INFO] {mode}  Weighted Avg Play = {avg_play:.2f} | Weighted Median = {median_play}")

        res_champ = []
        pc = (
            df[df["champion_name"] == target_champ]
            .groupby("summoner_id")
            .agg(plays=("champion_name", "size"), w=("w", "mean"))
        )
        all_w = df.groupby("summoner_id")["w"].mean()
        pc = pc.reindex(all_w.index, fill_value=0)
        pc["w"] = all_w

        avg_play = np.average(pc["plays"], weights=pc["w"])
        pc_sorted = pc.sort_values("plays")
        cum_w = pc_sorted["w"].cumsum()
        median_play = pc_sorted.loc[cum_w >= pc_sorted["w"].sum() / 2, "plays"].iloc[0]
        res_champ.append({"champion": target_champ, "avg_play_per_user": avg_play, "median_play_per_user": median_play})
        print(f"[INFO] {target_champ} Weighted Avg Play = {avg_play:.2f} | Weighted Median = {median_play}")

        avg_play_new_modes_df = pd.DataFrame(res_modes)
        avg_play_new_champ_df = pd.DataFrame(res_champ)
        conn.execute("CREATE OR REPLACE TABLE avg_play_new_modes AS SELECT * FROM avg_play_new_modes_df;")
        conn.execute("CREATE OR REPLACE TABLE avg_play_new_champ AS SELECT * FROM avg_play_new_champ_df;")
        del avg_play_new_modes_df, avg_play_new_champ_df

        # --- close connection ---
        conn.close()

        return ResponseDataAnalyze(
            **message.model_dump(),
            result="success",
        )


if __name__ == "__main__":
    # test
    component = Component()
    message = RequestDataAnalyze(
        duckdb_filepath="data/raw_data.db",
        report_filepath="data/report.db",
    )
    response = component(message)
    print(response)
