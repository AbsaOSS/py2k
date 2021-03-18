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
from copy import deepcopy
from typing import List, Dict, Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from py2k.models import KafkaModel


class ProducerConfig:
    def __init__(self, key, default_config, schema_registry_config, data):
        self._key = key
        self._default_config = default_config
        self._config_build = None

        self._serializer = KafkaSerializer(data[0], schema_registry_config)

    def get(self):
        if self._config_build:
            return self._config_build

        config_build = deepcopy(self._default_config)
        serializer_configs = {
            **self._value_serializer_config, **self._key_serializer_config}
        config_build.update(serializer_configs)

        self._config_build = config_build
        return self._config_build

    @property
    def _value_serializer_config(self):
        return {'value.serializer': self._serializer.value_serializer()}

    @property
    def _key_serializer_config(self):
        if not self._key:
            return {}

        return {'key.serializer': self._serializer.key_serializer(self._key)}


class KafkaSerializer:
    def __init__(self, item: KafkaModel, schema_registry_config: dict):
        self._item = item
        self._schema_registry_client = SchemaRegistryClient(
            schema_registry_config)

    def value_serializer(self):
        return AvroSerializer(
            self._item.value_schema_string,
            self._schema_registry_client,
            to_dict=self._results_to_dict
        )

    def key_serializer(self, key):
        return AvroSerializer(
            schema_str=self._key_schema_string(key),
            schema_registry_client=self._schema_registry_client
        )

    def _key_schema_string(self, key):
        key_schema = {}
        _value_schema = json.loads(self._item.schema_json())
        key_schema['type'] = _value_schema['type']
        key_schema['name'] = f'{_value_schema["name"]}Key'
        key_schema['namespace'] = _value_schema['namespace']
        key_schema['fields'] = self._find_key_fields(key, _value_schema['fields'])
        return str(key_schema).replace("'", "\"")

    @staticmethod
    def _find_key_fields(key, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        def is_key(field):
            return any(v == key for _, v in field.items())

        return [field for field in fields if is_key(field)]

    @staticmethod
    def _results_to_dict(results: KafkaModel, _):
        return json.loads(results.json())
