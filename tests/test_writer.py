"""
 Copyright 2021 ABSA Group Limited

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

from typing import Optional
from unittest.mock import ANY, MagicMock

import pytest
import datetime

import pandas as pd


import py2k.writer
from py2k.writer import KafkaWriter
from py2k.models import KafkaModel


@pytest.fixture
def data_class():
    class ModelResult(KafkaModel):
        Customerkey: str
        Predictedvalue: float
        Timesince: Optional[int]
        Applicableto: str
        Generationdate: datetime.date

    result = [
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
    output = [ModelResult(**value) for value in result]
    return output


@pytest.fixture
def pandas_dataframe():
    result = [
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
    return pd.DataFrame(result)


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


def test_writer_pushes_one_item_of_model_data(monkeypatch, data_class):
    topic = "DUMMY_TOPIC"
    key = "Customerkey"
    one_item = data_class[0]
    one_item_list = [one_item]

    producer_class = MagicMock()
    producer = MagicMock()
    producer_class.return_value = producer

    monkeypatch.setattr(py2k.writer, 'SerializingProducer', producer_class)
    monkeypatch.setattr(py2k.writer, 'SchemaRegistryClient', MagicMock())

    writer = KafkaWriter(topic, {}, {}, key)
    writer.write(one_item_list)

    expected_key = {key: getattr(one_item, key)}

    producer.produce.assert_called_with(topic=topic, key=expected_key, value=one_item, on_delivery=ANY)
    producer.poll.assert_called_with(0)
