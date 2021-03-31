from typing import Optional

import pytest
from pydantic.main import create_model

from py2k.record import KafkaRecord


def test_top_level_record_schema():
    MyRecord = create_model('MyRecord', a=(int, ...), __base__=KafkaRecord)
    schema = MyRecord(a=10).schema()

    expected = {
        'type': 'record',
        'name': 'MyRecord',
        'namespace': 'python.kafka.myrecord',
        'fields': [
            {'type': 'int', 'name': 'a'},
        ]
    }

    assert schema == expected


@pytest.mark.parametrize(
    'python_type,avro_type',
    [
        (bool, 'boolean'),
        (int, 'int'),
        (str, 'string'),
        (float, 'double')
    ]
)
def test_field_type(python_type, avro_type):
    MyRecord = create_model('MyRecord', a=(python_type, ...),
                            __base__=KafkaRecord)
    schema = MyRecord(a=python_type(1)).schema()
    field_type = schema['fields'][0]['type']

    assert field_type == avro_type


@pytest.mark.parametrize(
    'name',
    ['a', 'A', 'lower', 'Upper', 'camelCase', 'snake_case', 'WeirD_miSh_mAsh']
)
def test_field_name_unchanged(name):
    field_def = {name: 'default_string'}
    MyRecord = create_model('MyRecord', __base__=KafkaRecord, **field_def)
    schema = MyRecord(a=True).schema()

    name_in_schema = schema['fields'][0]['name']
    assert name_in_schema == name


@pytest.mark.parametrize(
    'python_value,avro_value',
    [
        (True, 'true'),
        (False, 'false'),
        (10, 10),
        ("dummy", 'dummy'),
        (10.2, 10.2)
    ]
)
def test_field_with_default(python_value, avro_value):
    MyRecord = create_model('MyRecord', a=python_value,  __base__=KafkaRecord)
    schema = MyRecord().schema()
    default_value = schema['fields'][0]['default']

    assert default_value == avro_value
