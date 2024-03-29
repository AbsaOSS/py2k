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

import datetime
from typing import Optional
from unittest.mock import ANY, MagicMock

import pandas as pd
import pytest

import py2k.producer_config
import py2k.producer
import py2k.serializer
from py2k.record import KafkaRecord
from py2k.writer import KafkaWriter


@pytest.fixture
def raw_input():
    return [
        {'Customerkey': 'Adam',
         'Predictedvalue': 123.22,
         'Timesince': 4,
         'Applicableto': '2020-07',
         'Generationdate': datetime.date(2020, 3, 20)},
        {'Customerkey': 'Andy',
         'Predictedvalue': 12545.0,
         'Timesince': 4,
         'Applicableto': '2020-09',
         'Generationdate': datetime.date(2020, 8, 2)},
    ]


@pytest.fixture
def first_value_dict_with_key():
    return {
        'Customerkey': 'Adam',
        'Predictedvalue': 123.22,
        'Timesince': 4,
        'Applicableto': '2020-07',
        'Generationdate': '2020-03-20'
    }


@pytest.fixture
def first_value_dict_without_key():
    return {
        'Predictedvalue': 123.22,
        'Timesince': 4,
        'Applicableto': '2020-07',
        'Generationdate': '2020-03-20'
    }


@pytest.fixture
def first_key_dict():
    return {'Customerkey': 'Adam'}


@pytest.fixture
def data_list(raw_input):
    class ModelResult(KafkaRecord):
        Customerkey: str
        Predictedvalue: float
        Timesince: Optional[int]
        Applicableto: str
        Generationdate: datetime.date

    output = [ModelResult(**value) for value in raw_input]
    return output


@pytest.fixture
def data_with_key_list(raw_input):
    class ModelResult(KafkaRecord):
        __key_fields__ = {'Customerkey'}
        Customerkey: str
        Predictedvalue: float
        Timesince: Optional[int]
        Applicableto: str
        Generationdate: datetime.date

    output = [ModelResult(**value) for value in raw_input]
    return output


@pytest.fixture
def pandas_dataframe(raw_input):
    return pd.DataFrame(raw_input)


@pytest.fixture
def producer(monkeypatch):
    producer_class = MagicMock()
    producer = MagicMock()
    producer_class.return_value = producer

    monkeypatch.setattr(py2k.producer, 'SerializingProducer', producer_class)
    monkeypatch.setattr(py2k.serializer,
                        'SchemaRegistryClient', MagicMock())

    return producer


def test_kafka_model(data_list):
    expected = {
        'type': 'record',
                'name': 'ModelResult',
                'namespace': 'python.kafka.modelresult',
                'fields': [
                    {'type': 'string', 'name': 'Customerkey'},
                    {'type': 'double', 'name': 'Predictedvalue'},
                    {'type': ['null', 'int'], 'name': 'Timesince'},
                    {'type': 'string', 'name': 'Applicableto'},
                    {'type': 'string', 'format': 'date',
                     'name': 'Generationdate'}]}
    actual = data_list[0].schema()
    assert actual == expected


def test_content(data_list):
    assert data_list[0].Customerkey == 'Adam'


def test_pandas_serializer(pandas_dataframe, data_list):
    class ModelResult(KafkaRecord):
        Customerkey: str
        Predictedvalue: float
        Timesince: int
        Applicableto: str
        Generationdate: datetime.date
    actual = ModelResult.from_pandas(pandas_dataframe)
    expected = data_list

    assert actual == expected


@pytest.mark.parametrize('iter_type', [list, iter])
def test_pushes_one_record(producer,
                           data_with_key_list,
                           first_value_dict_without_key,
                           first_key_dict,
                           iter_type):
    topic = "DUMMY_TOPIC"

    records = iter_type(data_with_key_list[:1])

    writer = KafkaWriter(topic, {}, {})
    writer.write(records)

    expected_key = first_key_dict

    producer.produce.assert_called_with(
        topic=topic, key=expected_key, value=first_value_dict_without_key,
        on_delivery=ANY)
    producer.poll.assert_called_with(0)


@pytest.mark.parametrize('iter_type', [list, iter])
def test_pushes_one_record_without_key(producer,
                                       data_list,
                                       first_value_dict_with_key,
                                       iter_type):
    topic = "DUMMY_TOPIC"
    records = iter_type(data_list[:1])

    writer = KafkaWriter(topic, {}, {})
    writer.write(records)

    producer.produce.assert_called_with(topic=topic,
                                        key=None,
                                        value=first_value_dict_with_key,
                                        on_delivery=ANY)
    producer.poll.assert_called_with(0)
