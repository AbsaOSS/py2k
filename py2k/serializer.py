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
    def __init__(self, item: KafkaModel, key, schema_registry_config: dict):
        self._item = item
        self._key = key
        self._schema_registry_client = SchemaRegistryClient(
            schema_registry_config)

    def value_serializer(self):
        return AvroSerializer(
            self._item.schema_json(),
            self._schema_registry_client,
            to_dict=self._to_dict_func()
        )

    def key_serializer(self):
        if not self._key:
            return None

        return AvroSerializer(
            schema_str=self._key_schema_string,
            schema_registry_client=self._schema_registry_client,
            to_dict=self._to_dict_func({self._key})
        )

    @property
    def _key_schema_string(self):
        key_schema = {}
        _value_schema = json.loads(self._item.schema_json())
        key_schema['type'] = _value_schema['type']
        key_schema['name'] = f'{_value_schema["name"]}Key'
        key_schema['namespace'] = _value_schema['namespace']
        key_schema['fields'] = self._find_key_fields(_value_schema['fields'])
        return str(key_schema).replace("'", "\"")

    def _find_key_fields(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        def is_key(field):
            return any(v == self._key for _, v in field.items())

        return [field for field in fields if is_key(field)]

    @staticmethod
    def _to_dict_func(include=None):
        def to_dict(results: KafkaModel, _):
            return json.loads(results.json(include=include))

        return to_dict
