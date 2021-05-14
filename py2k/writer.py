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
from itertools import tee
from typing import Any, Dict, Iterable

from tqdm import tqdm

from py2k.record import KafkaRecord
from py2k.producer_config import ProducerConfig
from py2k.producer import KafkaProducer
from py2k.serializer import KafkaSerializer


class KafkaWriter(object):
    def __init__(self,
                 topic: str,
                 schema_registry_config: Dict[str, Any],
                 producer_config: Dict[str, Any]):
        """A class for easy writing of data to kafka

        Args:
            topic (str): topic to post to
            schema_registry_config (Dict[str, Any]): a dictionary compatible
                with the `confluent_kafka.schema_registry.SchemaRegistryClient`
            producer_config (Dict[str, Any]): a dictionary compatible with the
                `confluent_kafka.SerializingProducer`
        """

        self._topic = topic
        self._producer_config = producer_config
        self._schema_registry_config = schema_registry_config
        self._producer = None

    def _create_producer(self, record: KafkaRecord):
        serializer = KafkaSerializer(record, self._schema_registry_config)
        producer_config = ProducerConfig(self._producer_config, serializer)
        self._producer = KafkaProducer(self._topic, producer_config)

    def write(self, records: Iterable[KafkaRecord], verbose: bool = False):
        """Writes data to Kafka.

        Args:
            records (Iterable[KafkaRecord]): Serialized `KafkaModel` objects
            verbose (bool): Whether or not you want to show the loading bar

        Examples:
            >>> from py2k.writer import KafkaWriter
            >>> writer = KafkaWriter(topic=topic,
                     schema_registry_config=schema_registry_config,
                     producer_config=producer_config)
            >>> writer.write(records)
            100%|██████████| 4/4 [00:00<00:00,  7.69it/s]
        """

        for_first, records_copy = tee(records)
        self._create_producer(next(for_first))

        for record in (tqdm(records_copy) if verbose else records_copy):
            self._producer.produce(record)

        self._producer.flush()
