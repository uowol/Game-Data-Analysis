from abc import ABC, abstractmethod
from pydantic import BaseModel


class PipelineType(BaseModel):
    pass


class Pipeline(ABC):
    def __init__(self, **config):
        self.init(**config)
    
    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)
    
    @abstractmethod
    def init(self, **config):
        pass
    
    @abstractmethod
    def call(self, *args, **kwargs):
        pass