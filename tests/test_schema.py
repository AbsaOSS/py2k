import pytest
from pydantic.main import create_model

from py2k.record import KafkaRecord


@pytest.mark.parametrize(
    'python_type,avro_type',
    [
        (bool, 'boolean'),
        (int, 'int'),
        (str, 'string'),
        (float, 'double')
    ]
)
def test_schema_without_default(python_type, avro_type):
    MyRecord = create_model('MyRecord', a=(python_type, ...),
                            __base__=KafkaRecord)
    schema = MyRecord(a=True).schema()

    expected = {
        'type': 'record',
        'name': 'MyRecord',
        'namespace': 'python.kafka.myrecord',
        'fields': [
            {'type': avro_type, 'name': 'a'},
        ]
    }

    assert schema == expected


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
