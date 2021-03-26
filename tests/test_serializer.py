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


import json

import pandas as pd
import pytest

import py2k.serializer
from py2k.models import KafkaModel
from py2k.serializer import KafkaSerializer


class ParamMock:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def test_schema_string_of_value_serializer(serializer_without_key):
    value_serializer = serializer_without_key.value_serializer()

    schema = json.loads(value_serializer.kwargs.get('schema_str'))
    expected_schema = {
        'fields': [{'name': 'Field', 'type': 'string'}],
        'name': 'ModelResult',
        'namespace': 'python.kafka.modelresult',
        'type': 'record'
    }
    assert schema == expected_schema


def test_key_serializer_none_if_no_key_fiedls(serializer_without_key):
    key_serializer = serializer_without_key.key_serializer()
    assert key_serializer is None


def test_schema_string_of_key_serializer(serializer_with_key):
    key_serializer = serializer_with_key.key_serializer()
    schema = json.loads(key_serializer.kwargs.get('schema_str'))

    expected_schema = {
        'fields': [{'name': 'Key', 'type': 'string'}],
        'name': 'ModelResultKey',
        'namespace': 'python.kafka.modelresult',
        'type': 'record'
    }
    assert schema == expected_schema


def test_schema_string_of_multi_key_serializer(serializer_with_multiple_key):
    key_serializer = serializer_with_multiple_key.key_serializer()
    schema = json.loads(key_serializer.kwargs.get('schema_str'))

    expected_schema = {
        'fields': [{'name': 'Key1', 'type': 'string'},
                   {'name': 'Key2', 'type': 'string'}],
        'name': 'ModelResultKey',
        'namespace': 'python.kafka.modelresult',
        'type': 'record'
    }
    assert schema == expected_schema


def test_value_serializer_without_key_by_default(serializer_with_multiple_key):
    value_serializer = serializer_with_multiple_key.value_serializer()

    schema = json.loads(value_serializer.kwargs.get('schema_str'))
    expected_schema = {
        'fields': [{'name': 'Field', 'type': 'string'}],
        'name': 'ModelResult',
        'namespace': 'python.kafka.modelresult',
        'type': 'record'
    }
    assert schema == expected_schema


def test_value_serializer_with_key_when_specified(serializer_key_included):
    value_serializer = serializer_key_included.value_serializer()

    schema = json.loads(value_serializer.kwargs.get('schema_str'))
    expected_schema = {
        'fields': [{'name': 'Field', 'type': 'string'},
                   {'name': 'Key1', 'type': 'string'},
                   {'name': 'Key2', 'type': 'string'}],
        'name': 'ModelResult',
        'namespace': 'python.kafka.modelresult',
        'type': 'record'
    }
    assert schema == expected_schema


@pytest.fixture
def serializer_without_key(monkeypatch, schema_registry_config):
    monkeypatch.setattr(py2k.serializer, 'AvroSerializer', ParamMock)

    class ModelResult(KafkaModel):
        Field: str

    df = pd.DataFrame({'Field': ['field_value']})
    record = ModelResult.from_pandas(df)[0]

    return KafkaSerializer(record, schema_registry_config)


@pytest.fixture
def serializer_with_key(monkeypatch, schema_registry_config):
    monkeypatch.setattr(py2k.serializer, 'AvroSerializer', ParamMock)

    class ModelResult(KafkaModel):
        __key_fields__ = ['Key']
        Field: str
        Key: str

    df = pd.DataFrame({'Field': ['field_value'], 'Key': ['key_value']})
    record = ModelResult.from_pandas(df)[0]

    return KafkaSerializer(record, schema_registry_config)


@pytest.fixture
def serializer_with_multiple_key(monkeypatch, schema_registry_config):
    monkeypatch.setattr(py2k.serializer, 'AvroSerializer', ParamMock)

    class ModelResult(KafkaModel):
        __key_fields__ = ['Key1', 'Key2']
        Field: str
        Key1: str
        Key2: str

    df = pd.DataFrame({
        'Field': ['field_value'],
        'Key1': ['key1_value'],
        'Key2': ['key2_value']
    })
    record = ModelResult.from_pandas(df)[0]

    return KafkaSerializer(record, schema_registry_config)


@pytest.fixture
def serializer_key_included(monkeypatch, schema_registry_config):
    monkeypatch.setattr(py2k.serializer, 'AvroSerializer', ParamMock)

    class ModelResult(KafkaModel):
        __key_fields__ = ['Key1', 'Key2']
        __key_included__ = True
        Field: str
        Key1: str
        Key2: str

    df = pd.DataFrame({
        'Field': ['field_value'],
        'Key1': ['key1_value'],
        'Key2': ['key2_value']
    })
    record = ModelResult.from_pandas(df)[0]

    return KafkaSerializer(record, schema_registry_config)


@pytest.fixture
def schema_registry_config():
    return {
      'url': "http://test.schema.registry"
    }
