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
from py2k.models import KafkaModel
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
        {'Customerkey': 'Daniel',
         'Predictedvalue': 123,
         'Timesince': 4,
         'Applicableto': '2020-03',
         'Generationdate': datetime.date(2020, 12, 9)},
        {'Customerkey': 'Dennis',
         'Predictedvalue': 44123.02,
         'Timesince': 4,
         'Applicableto': '2020-10',
         'Generationdate': datetime.date(2020, 4, 21)},
        {'Customerkey': 'Felipe',
         'Predictedvalue': 11111,
         'Timesince': 4,
         'Applicableto': '2020-01',
         'Generationdate': datetime.date(2020, 8, 17)},
    ]


@pytest.fixture
def serialized_first_input():
    return {
        'Customerkey': 'Adam',
        'Predictedvalue': 123.22,
        'Timesince': 4,
        'Applicableto': '2020-07',
        'Generationdate': '2020-03-20'
    }

@pytest.fixture
def data_class(raw_input):
    class ModelResult(KafkaModel):
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


def test_kafka_model(data_class):
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
    actual = data_class[0].schema()
    assert actual == expected


def test_content(data_class):
    assert data_class[0].Customerkey == 'Adam'


def test_pandas_serializer(pandas_dataframe, data_class):
    class ModelResult(KafkaModel):
        Customerkey: str
        Predictedvalue: float
        Timesince: int
        Applicableto: str
        Generationdate: datetime.date
    actual = ModelResult.from_pandas(pandas_dataframe)
    expected = data_class

    assert actual == expected


def test_pushes_one_item_of_model_data(monkeypatch, data_class,
                                       serialized_first_input):
    topic = "DUMMY_TOPIC"
    key = "Customerkey"

    one_item_list = data_class[:1]

    producer_class = MagicMock()
    producer = MagicMock()
    producer_class.return_value = producer

    monkeypatch.setattr(py2k.producer, 'SerializingProducer', producer_class)
    monkeypatch.setattr(py2k.serializer,
                        'SchemaRegistryClient', MagicMock())

    writer = KafkaWriter(topic, {}, {}, key)
    writer.write(one_item_list)

    expected_key = {key: serialized_first_input[key]}

    producer.produce.assert_called_with(
        topic=topic, key=expected_key, value=serialized_first_input,
        on_delivery=ANY)
    producer.poll.assert_called_with(0)


def test_pushes_one_item_of_model_data_without_key(monkeypatch, data_class,
                                                   serialized_first_input):
    topic = "DUMMY_TOPIC"
    one_item_list = data_class[:1]

    producer_class = MagicMock()
    producer = MagicMock()
    producer_class.return_value = producer

    monkeypatch.setattr(py2k.producer, 'SerializingProducer', producer_class)
    monkeypatch.setattr(py2k.serializer,
                        'SchemaRegistryClient', MagicMock())

    writer = KafkaWriter(topic, {}, {})
    writer.write(one_item_list)

    producer.produce.assert_called_with(
        topic=topic, key=None, value=serialized_first_input, on_delivery=ANY)
    producer.poll.assert_called_with(0)
