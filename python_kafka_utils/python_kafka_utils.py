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

import json
from typing import Any, Dict, List, Union
from uuid import uuid4

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from tqdm import tqdm

from python_kafka_utils.models import KafkaModel, IterableAdapter


class KafkaWriter(object):
    def __init__(self,
                 topic: str,
                 schema_registry_config: Dict[str, Any],
                 producer_config: Dict[str, Any],
                 result_class: Union[KafkaModel, List[KafkaModel]],
                 key=None):
        """A class for easy writing of data to kafka

        :param schema_registry_config: a dictionary compatible with the
         `confluent_kafka.schema_registry.SchemaRegistryClient`
        :type schema_registry_config: dict
        :param producer_config: a dictionary compatible with the
         `confluent_kafka.SerializingProducer`
        :type producer_config: dict
        :param result_class: A statically typed result class
        :type result_class: Union[object, List[object]]
        """
        self.topic = topic
        self.schema_registry_client = SchemaRegistryClient(
            schema_registry_config)
        self.producer_config = producer_config
        self._results = result_class
        self.key = key

    def __del__(self):
        if self.producer:
            self.producer.flush()

    @staticmethod
    def _results_to_dict(results: KafkaModel, ctx):
        return json.loads(results.json())

    @staticmethod
    def delivery_report(err, msg):
        """
        Reports the failure or success of a message delivery.

        Note:
            In the delivery report callback the Message.key()
            and Message.value() will be the binary format as
            encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s)
            for key and value to delivery
            report callback we recommend a bound callback
            or lambda where you pass the objects along.

        :param err: The error that occurred on None on success.
        :type err: KafkaError
        :param msg: The message that was produced or failed.
        :type msg: Message
        """
        if err is not None:
            print(
                "Delivery failed for record {}: {}".format(msg.key(), err)
            )

    def _create_value_serializer(self):
        self.value_schema_string = self._get_schema_string(self._results)
        self.avro_value_serializer = AvroSerializer(
            self.value_schema_string,
            self.schema_registry_client,
            to_dict=self._results_to_dict)
        self.producer_config.update({
            'value.serializer': self.avro_value_serializer
        })

    def _create_key_serializer(self):
        self._create_key_schema()
        self.avro_key_serializer = AvroSerializer(
            schema_str=self.key_schema_string,
            schema_registry_client=self.schema_registry_client
        )
        self.producer_config.update({
            'key.serializer': self.avro_key_serializer
        })

    def create_producer(self):
        self._create_value_serializer()
        if self.key:
            self._create_key_serializer()
        self.producer = SerializingProducer(self.producer_config)

    def _create_key_schema(self):
        key_schema = dict()
        _value_schema = json.loads(self.value_schema_string)
        key_schema['type'] = _value_schema['type']
        key_schema['name'] = f'{_value_schema["name"]}Key'
        key_schema['namespace'] = _value_schema['namespace']
        key_schema['fields'] = self._resolve_key_fields_(
            _value_schema['fields'], self.key
        )
        print(key_schema)
        self.key_schema_string = str(key_schema).replace("'", "\"")

    @staticmethod
    def _resolve_key_fields_(fields: List[Dict[str, Any]],
                             key: str) -> Dict[str, Any]:
        final = list()
        for field in fields:
            for _, v in field.items():
                if v == key:
                    final.append(field)
        return final

    def set_key(self, key: str):
        self.key = key

    def _assign_key(self):
        if not self.key:
            return str(uuid4())

    def write(self):
        result = self._results
        if isinstance(result, list) or isinstance(result, IterableAdapter):
            self._write_list(result)
        elif isinstance(result, KafkaModel):
            self._write_one(result)

    def _write_list(self, results: List[KafkaModel]):
        for result in tqdm(results):
            self._to_kafka(result)

    def _to_kafka(self, result: KafkaModel):
        while True:
            try:
                if self.key:
                    key = {self.key: result.dict()[self.key]}
                else:
                    key = self._assign_key()
                self.producer.produce(topic=self.topic,
                                      key=key,
                                      value=result,
                                      on_delivery=self.delivery_report)
                self.producer.poll(0)
                break
            except BufferError as e:
                print(
                    f'Failed to send on attempt {key}. Error received {str(e)}')
                self.producer.poll(1)

    def _write_one(self, result: KafkaModel):
        self._to_kafka(result)

    @staticmethod
    def _get_schema_string(results: Union[List[KafkaModel], KafkaModel]):
        if isinstance(results, list):
            return results[0].schema_json()
        if isinstance(results, IterableAdapter):
            return KafkaModel.schema_from_iter(results)
        else:
            return results.schema_json()
