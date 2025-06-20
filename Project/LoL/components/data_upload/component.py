import os
import yaml
import pandas as pd
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.storage import duckdb
from components.formats import RequestDuckdbDataUpload, ResponseDuckdbDataUpload


class ComponentType(base.ComponentType):
    pass


class Component(base.Component):
    alias = "data_upload"

    def init(self, **config):
        if config:
            self.config = ComponentType(**config)
        else:
            with open(os.path.join(Path(__file__).parent, "config.yaml"), "r") as fp:
                self.config = yaml.safe_load(fp)
                self.config = self.config if self.config is not None else {}

    def call(self, message: RequestDuckdbDataUpload, *args, **kwargs) -> ResponseDuckdbDataUpload:
        # --- get duckdb connection ---
        conn = duckdb.get_connection(message.duckdb_filepath)

        # --- init database ---
        table = "raw_summoner_game_logs"
        tables = duckdb.ls_table(conn)
        if table in tables:
            print(f"# [INFO] drop table: {table}")
            conn.execute(f"DROP TABLE {table};")
        print(f"# [INFO] create table: {table}")
        duckdb.excute_query(conn, self.config["query"]["create"]["table"][table])
        duckdb.excute_query(conn, self.config["query"]["insert"][table].format(f"{message.chunks_dir}/{message.date}/*.parquet"))
        
        # --- close connection ---
        conn.close()

        return ResponseDuckdbDataUpload(
            **message.model_dump(),
            result="success",
        )


if __name__ == "__main__":
    # test
    component = Component()
    message = RequestDuckdbDataUpload(
        duckdb_filepath="data/raw_data.db",
        chunks_dir="data/chunks/2025-05-14",
    )
    response = component(message)
    print(response)
