import os
import yaml
import pandas as pd
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.storage import duckdb
from components.formats import RequestDataDelete, ResponseDataDelete


class ComponentType(base.ComponentType):
    pass


class Component(base.Component):
    alias = "data_delete"

    def init(self, **config):
        if config:
            self.config = ComponentType(**config)
        else:
            with open(os.path.join(Path(__file__).parent, "config.yaml"), "r") as fp:
                self.config = yaml.safe_load(fp)
                self.config = self.config if self.config is not None else {}

    def call(self, message: RequestDataDelete, *args, **kwargs) -> ResponseDataDelete:
        # 만약 이전 collect component에서 실패, 어디서? 를 받으면 해당 티어의 유저 데이터(parquet)를 삭제
        # --- get duckdb connection ---
        conn = duckdb.get_connection()

        # --- get parquet file list ---
        remove_list = (
            conn.execute(
                f"SELECT summoner_id FROM '{message.shards_dir}/*.parquet' WHERE tier='{message.tier}' AND rank='{message.division}'"
            )
            .fetchdf()
            .summoner_id.unique()
            .tolist()
        )
        print(f"[INFO] Will Remove {len(remove_list)} shards: {message.tier} {message.division}.")

        # --- remove parquet file ---
        for summoner_id in remove_list:
            file_path = Path(message.shards_dir) / f"{summoner_id}.parquet"
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[INFO] Remove {file_path}.")

        return ResponseDataDelete(
            **message.model_dump(),
            result="success",
        )


if __name__ == "__main__":
    # test
    component = Component()
    message = RequestDataDelete(
        shards_dir="data/shards",
        tier="EMERALD",
        division="III",
    )
    response = component(message)
    print(response)
