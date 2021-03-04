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

from typing import Any, Dict

from tqdm import tqdm

from py2k.producer_config import ProducerConfig
from py2k.serializer import KafkaSerializer


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
        """
        self._topic = topic
        self._default_producer_config = producer_config
        self._key = key
        self._schema_registry_config = schema_registry_config
        self._serializer = None

    def __del__(self):
        if self.producer:
            self.producer.flush()

    def _create_serializer(self, data):
        producer_config = ProducerConfig(self._key, self._default_producer_config, self._schema_registry_config, data)
        self._serializer = KafkaSerializer(self._topic, self._key, producer_config)

    def write(self, data):
        self._create_serializer(data)
        for item in tqdm(data):
            self._serializer.produce(item)
