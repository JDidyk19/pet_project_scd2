import uuid
from dataclasses import dataclass, field
from datetime import datetime, date

from faker import Faker

from base import BaseModel


faker = Faker()


@dataclass
class Person(BaseModel):
    
    first_name: str
    last_name: str
    birthday: datetime
    email: str
    created_at: datetime
    updated_at: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4) # generate uniq id
    ts_db: datetime = field(init=False)
    
    def __post_init__(self):
        self.ts_db = datetime.now()
    
    @classmethod
    def generate_person(cls):
        first_date = faker.date_time_ad(
            start_datetime=date(year=2025, month=1, day=1),
            end_datetime=date(year=2026, month=3, day=15)
        )
        
        return cls(
            first_name=faker.first_name(),
            last_name=faker.last_name(),
            birthday=faker.date_time_ad(
                start_datetime=date(year=1980, month=1, day=1),
                end_datetime=date(year=2015, month=1, day=1)
            ),
            email=faker.email(),
            created_at=first_date,
            updated_at=first_date
        )
