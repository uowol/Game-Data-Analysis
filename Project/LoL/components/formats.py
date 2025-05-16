from pydantic import BaseModel
from typing import List, Dict, Union, Optional


class RequestMessage(BaseModel):
    pass


class ResponseMessage(BaseModel):
    pass


class RecipeItem(BaseModel):
    tier: str
    division: Optional[str]
    ratio: float


class RequestDataCollect(RequestMessage):
    date: str
    queue: str
    shards_dir: str
    sample_size: int
    recipe: List[RecipeItem]
    resume: bool = False


class ResponseDataCollect(RequestDataCollect):
    result: str  # "success" or "failed"


class RequestDataDelete(RequestMessage):
    shards_dir: str
    tier: str
    division: Optional[str]


class ResponseDataDelete(RequestDataDelete):
    result: str  # "success" or "failed"


class RequestDuckdbDataUpload(RequestMessage):
    shards_dir: str
    duckdb_filepath: str


class ResponseDuckdbDataUpload(RequestDuckdbDataUpload):
    result: str  # "success" or "failed"


class RequestDataAnalyze(RequestMessage):
    duckdb_filepath: str
    report_filepath: str


class ResponseDataAnalyze(RequestDataAnalyze):
    result: str  # "success" or "failed"
