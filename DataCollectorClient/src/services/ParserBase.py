from abc import ABC,abstractmethod

class ParserBase(ABC):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError
        
    @abstractmethod
    def download(self, *args, **kwargs)-> dict: 
        raise NotImplementedError
    
    @abstractmethod
    def update(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def should_update(self, *args, **kwargs) -> bool:
        raise NotImplementedError
        
    f
    
    