from dataclasses import dataclass, asdict


@dataclass
class BaseModel:
    
    def to_dict(self):
        return asdict(self)
    