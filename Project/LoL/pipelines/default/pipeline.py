from typing import Optional
from pipelines import base
from components.data_collect.component import Component as DataCollectComponent
from components.data_upload.component import Component as DataUploadComponent
from components.data_analyze.component import Component as DataAnalyzeComponent
from components.formats import (
    RequestMessage,
    ResponseMessage,
    RequestDataCollect,
    ResponseDataCollect,
    RequestDuckdbDataUpload,
    ResponseDuckdbDataUpload,
    RequestDataAnalyze,
    ResponseDataAnalyze,
)


class PipelineType(base.PipelineType):
    data_collect: Optional[RequestDataCollect] = None
    data_upload: Optional[RequestDuckdbDataUpload] = None
    data_analyze: Optional[RequestDataAnalyze] = None


class Pipeline(base.Pipeline):
    def init(self, **config):
        self.config = PipelineType(**config)  # pydantic validation

    def call(self):
        def exec_component(component, request_message: RequestMessage) -> ResponseMessage:
            print(f"# ===== exec_component: {component.__class__.__name__} =====")
            print("# [INFO] request_message: ", request_message)
            response_message = component(request_message)
            print("# [INFO] response_message: ", response_message)
            assert response_message.result == "success", f"exec_component failed: {response_message}"
            return response_message

        upstream_events = []
        if self.config.data_collect is not None:
            request_message = self.config.data_collect
            response_message = exec_component(DataCollectComponent(), request_message)
            upstream_events.append(response_message.model_dump())
        if self.config.data_upload is not None:
            request_message = RequestDuckdbDataUpload(
                upstream_events=upstream_events,
                chunks_dir=self.config.data_upload.chunks_dir,
                duckdb_filepath=self.config.data_upload.duckdb_filepath,
            )
            response_message = exec_component(DataUploadComponent(), request_message)
            upstream_events.append(response_message.model_dump())
        if self.config.data_analyze is not None:
            request_message = RequestDataAnalyze(
                upstream_events=upstream_events,
                duckdb_filepath=self.config.data_analyze.duckdb_filepath,
                report_filepath=self.config.data_analyze.report_filepath,
            )
            response_message = exec_component(DataAnalyzeComponent(), request_message)
            upstream_events.append(response_message.model_dump())
