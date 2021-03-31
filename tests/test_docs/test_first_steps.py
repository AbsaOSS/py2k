import datetime
from py2k.writer import KafkaWriter

import pytest
from py2k.record import KafkaRecord

from docs_src.first_steps.tutorial001 import MyRecord
from docs_src.first_steps.tutorial002 import writer


@pytest.fixture
def record():
    test_dict = {'name': 'Daniel',
                 'age': 27,
                 'birthday': datetime.date(1993, 9, 4)}
    return MyRecord(**test_dict)


def test_record_works(record):

    assert isinstance(record, KafkaRecord)


def test_tutorial2():
    assert writer._topic == 'dummy_topic'
    assert writer._schema_registry_config == {
        'url': 'http://myschemaregistry.com:8081'}
    assert writer._producer_config == {
        'bootstrap.servers': 'myproducer.com:9092'}
    assert isinstance(writer, KafkaWriter)
