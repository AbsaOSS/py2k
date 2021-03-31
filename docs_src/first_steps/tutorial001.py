import datetime
from py2k.record import KafkaRecord


class MyRecord(KafkaRecord):
    name: str
    age: int
    birthday: datetime.date
