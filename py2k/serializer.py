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


from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from py2k.models import KafkaRecord


class KafkaSerializer:
    def __init__(self, record: KafkaRecord, schema_registry_config: dict):
        self._record = record
        self._key_fields = record.key_fields
        self._key_included = record.key_included
        self._schema_registry_client = SchemaRegistryClient(
            schema_registry_config)

    def value_serializer(self):
        return AvroSerializer(
            schema_str=self._record.value_schema_string,
            schema_registry_client=self._schema_registry_client,
        )

    def key_serializer(self):
        if not self._key_fields:
            return None

        return AvroSerializer(
            schema_str=self._record.key_schema_string,
            schema_registry_client=self._schema_registry_client
        )
