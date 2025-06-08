from pydantic import BaseModel
from typing import List, Dict, Union, Optional


class ResponseMessage(BaseModel):
    result: str  # "success" or "fail"


class RequestMessage(BaseModel):
    upstream_events: List[dict] = []


class RecipeItem(BaseModel):
    tier: str
    division: Optional[str]
    ratio: float


class RequestDataCollect(RequestMessage):
    date: str
    queue: str
    chunks_dir: str
    sample_size: int
    recipe: List[RecipeItem]
    resume: bool = False


class ResponseDataCollect(ResponseMessage, RequestDataCollect):
    pass


class RequestDataDelete(RequestMessage):
    chunks_dir: Optional[str] = None
    tier: Optional[str] = None
    division: Optional[str] = None


class ResponseDataDelete(ResponseMessage, RequestDataDelete):
    pass


class RequestDuckdbDataUpload(RequestMessage):
    date: str
    chunks_dir: Optional[str] = None
    duckdb_filepath: Optional[str] = None


class ResponseDuckdbDataUpload(ResponseMessage, RequestDuckdbDataUpload):
    pass


class RequestDataAnalyze(RequestMessage):
    duckdb_filepath: Optional[str] = None
    report_filepath: Optional[str] = None


class ResponseDataAnalyze(ResponseMessage, RequestDataAnalyze):
    pass


class RequestDashboardRun(RequestMessage):
    build: bool = False
    container_name: Optional[str] = None
    port: Optional[int] = None


class ResponseDashboardRun(ResponseMessage, RequestDashboardRun):
    pass
