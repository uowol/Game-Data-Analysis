from pydantic import BaseModel
from typing import List, Dict, Union, Optional


class RequestMessage(BaseModel):
    pass

class ResponseMessage(BaseModel):
    pass

class RequestDataCollect(RequestMessage):
    queue: str
    tier: str
    division: str
    output_dir: str

class ResponseDataCollect(RequestDataCollect):
    pass