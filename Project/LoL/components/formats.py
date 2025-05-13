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
    queue: str
    output_dir: str
    sample_size: int
    recipe: List[RecipeItem]


class ResponseDataCollect(RequestDataCollect):
    pass
