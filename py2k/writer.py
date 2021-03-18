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

from typing import Any, Dict, List

from tqdm import tqdm

from py2k.models import KafkaModel
from py2k.producer_config import ProducerConfig
from py2k.producer import KafkaProducer


class KafkaWriter(object):
    def __init__(self,
                 topic: str,
                 schema_registry_config: Dict[str, Any],
                 producer_config: Dict[str, Any],
                 key: str = None):
        """A class for easy writing of data to kafka

        Args:
            topic (str): topic to post to
            schema_registry_config (Dict[str, Any]): a dictionary compatible
                with the `confluent_kafka.schema_registry.SchemaRegistryClient`
            producer_config (Dict[str, Any]): a dictionary compatible with the
                `confluent_kafka.SerializingProducer`
            key (str, optional): [description]. Defaults to None.
        """

        self._topic = topic
        self._producer_config = producer_config
        self._key = key
        self._schema_registry_config = schema_registry_config
        self._producer = None

    def _create_producer(self, data: List[KafkaModel]):
        producer_config = ProducerConfig(
            self._key,
            self._producer_config,
            self._schema_registry_config,
            data[0]
        )

        self._producer = KafkaProducer(self._topic, producer_config)

    def write(self, data: List[KafkaModel]):
        """writes data to Kafka

        Args:
            data (List[KafkaModel]): Serialized `KafkaModel` objects
        """
        self._create_producer(data)
        for item in tqdm(data):
            self._producer.produce(item)

        self._producer.flush()
