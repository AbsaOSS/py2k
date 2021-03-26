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
from typing import List, Dict, Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from py2k.models import KafkaModel


class KafkaSerializer:
    def __init__(self, record: KafkaModel, schema_registry_config: dict):
        self._record = record
        self._key_fields = record.key_fields
        self._schema_registry_client = SchemaRegistryClient(
            schema_registry_config)

    def value_serializer(self):
        return AvroSerializer(
            schema_str=self._value_schema_string,
            schema_registry_client=self._schema_registry_client,
        )

    def key_serializer(self):
        if not self._key_fields:
            return None

        return AvroSerializer(
            schema_str=self._key_schema_string,
            schema_registry_client=self._schema_registry_client
        )

    @property
    def _value_schema_string(self):
        _value_schema = json.loads(self._record.schema_json())
        if self._key_fields:
            fields = _value_schema['fields']
            _value_schema['fields'] = self._find_val_fields(fields)

        return str(_value_schema).replace("'", "\"")

    @property
    def _key_schema_string(self):
        key_schema = {}
        _value_schema = json.loads(self._record.schema_json())
        key_schema['type'] = _value_schema['type']
        key_schema['name'] = f'{_value_schema["name"]}Key'
        key_schema['namespace'] = _value_schema['namespace']
        key_schema['fields'] = self._find_key_fields(_value_schema['fields'])
        return str(key_schema).replace("'", "\"")

    def _is_key(self, field):
        return field.get('name') in self._key_fields

    def _find_val_fields(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        return [field for field in fields if not self._is_key(field)]

    def _find_key_fields(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        return [field for field in fields if self._is_key(field)]
