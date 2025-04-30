from typing import Optional
from pipelines import base
from components.formats import RequestDataCollect, ResponseDataCollect
from components.data_collect.component import Component as DataCollectComponent


class PipelineType(base.PipelineType):
    data_collect: Optional[RequestDataCollect] = None


class Pipeline(base.Pipeline):
    def init(self, **config):
        self.config = PipelineType(**config)  # pydantic validation

    def call(self):
        upstream_events = []
        if self.config.data_collect is not None:
            print("\n# [INFO] =============== data_collect start ===============")
            component = DataCollectComponent()
            request_message = self.config.data_collect
            print("# [INFO] request_message: ", request_message)
            response_message = component(request_message)
            print("# [INFO] response_message: ", response_message)
            upstream_events.append(response_message)
            print("# [INFO] =============== data_collect end ===============")
