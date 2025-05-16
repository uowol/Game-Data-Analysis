import os
import yaml
import pandas as pd
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
from components import base
from modules.storage import duckdb
from components.formats import RequestDashboardRun, ResponseDashboardRun


class ComponentType(base.ComponentType):
    pass


class Component(base.Component):
    alias = "dashboard_run"

    def init(self, **config):
        if config:
            self.config = ComponentType(**config)
        else:
            with open(os.path.join(Path(__file__).parent, "config.yaml"), "r") as fp:
                self.config = yaml.safe_load(fp)
                self.config = self.config if self.config is not None else {}

    def call(self, message: RequestDashboardRun, *args, **kwargs) -> ResponseDashboardRun:
        if message.build:
            duckdb.docker_build_metabase()
            print("# [INFO] Docker metabase container build complete.")

        duckdb.docker_run_metabase(
            container_name=message.container_name,
            port=message.port,
        )
        print("# [INFO] Dashboard run complete.")

        return ResponseDashboardRun(
            **message.model_dump(),
            result="success",
        )
