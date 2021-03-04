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

from typing import Any, Dict, List
from uuid import uuid4

from confluent_kafka.serializing_producer import SerializingProducer
from tqdm import tqdm

from py2k._writer_config import ProducerConfig
from py2k.models import KafkaModel


class KafkaWriter(object):
    def __init__(self,
                 topic: str,
                 schema_registry_config: Dict[str, Any],
                 producer_config: Dict[str, Any],
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
        self.producer_config = producer_config
        self.key = key
        self._schema_registry_config = schema_registry_config
        self._serializer = KafkaSerializer(key)

    def __del__(self):
        if self.producer:
            self.producer.flush()

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
                "Delivery failed for record {}: {}".format(msg.serialize_key(), err)
            )

    def _create_producer(self, data):
        producer_config = ProducerConfig(self.key, self.producer_config, self._schema_registry_config, data)
        self.producer = SerializingProducer(producer_config.get())

    def write(self, data):
        self._create_producer(data)

        self._write_list(data)

    def _write_list(self, results: List[KafkaModel]):
        for result in tqdm(results):
            self._to_kafka(result)

    def _to_kafka(self, result: KafkaModel):
        while True:
            try:
                key = self._serializer.serialize_key(result)
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


class KafkaSerializer:
    def __init__(self, key):
        self._key = key

    def serialize_key(self, item):
        if self._key:
            return {self._key: item.dict()[self._key]}
        else:
            return self._assign_key()

    @property
    def producer(self):
        return self._producer

    def _assign_key(self):
        if not self._key:
            return str(uuid4())
