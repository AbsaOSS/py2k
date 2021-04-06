# Copyright 2021 ABSA Group Limited

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


_types_to_test = [
    (bool, 'boolean'),
    (int, 'int'),
    (str, 'string'),
    (float, 'double'),
]


@pytest.mark.parametrize('python_type,avro_type', _types_to_test)
def test_field_type(python_type, avro_type):
    MyRecord = create_model('MyRecord', a=(python_type, ...),
                            __base__=KafkaRecord)

    record = MyRecord(a=python_type(1))
    schema = record.schema()
    field_type = schema['fields'][0]['type']

    assert field_type == avro_type


@pytest.mark.parametrize('python_type,avro_type', _types_to_test)
def test_optional_field_type(python_type, avro_type):
    MyRecord = create_model('MyRecord',
                            a=(Optional[python_type], None),
                            __base__=KafkaRecord)

    record = MyRecord()
    schema = record.schema()
    field_type = schema['fields'][0]['type']

    assert field_type == ['null', avro_type]


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
