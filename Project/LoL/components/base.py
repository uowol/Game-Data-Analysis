from abc import ABC, abstractmethod
from pydantic import BaseModel

from components.formats import RequestMessage, ResponseMessage


class ComponentType(BaseModel):
    pass


class Component(ABC):
    def __init__(self, **config):
        self.init(**config)

    def __call__(self, request: RequestMessage, *args, **kwargs) -> ResponseMessage:
        res = None
        merged_request = self.merge_upstream_request(request)
        try:
            res = self.call(merged_request, *args, **kwargs)
        except Exception as e:
            res = ResponseMessage(result="fail")
            print(f"# [ERROR] {e}")
        return res

    def merge_upstream_request(self, request: RequestMessage) -> RequestMessage:
        request_data = request.model_dump()
        for upstream_data in reversed(request.upstream_events):
            for key, value in upstream_data.items():
                if request_data.get(key) is None and value is not None:
                    request_data[key] = value
        return request.__class__(**request_data)

    @abstractmethod
    def init(self, **config):
        pass

    @abstractmethod
    def call(self, request: RequestMessage, *args, **kwargs) -> ResponseMessage:
        pass
