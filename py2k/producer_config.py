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
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        self._data = data
        self._value_schema_string = self._get_schema_string(self._data[0])
        self._config_build = None

    def get(self):
        if self._config_build:
            return self._config_build

        config_build = deepcopy(self._default_config)
        serializer_configs = {**self._value_serializer_config, **self._key_serializer_config}
        config_build.update(serializer_configs)

        self._config_build = config_build
        return self._config_build

    @property
    def _value_serializer_config(self):
        avro_value_serializer = AvroSerializer(
            self._value_schema_string,
            self.schema_registry_client,
            to_dict=self._results_to_dict
        )
        return {'value.serializer': avro_value_serializer}

    @property
    def _key_serializer_config(self):
        if not self._key:
            return {}

        avro_key_serializer = AvroSerializer(
            schema_str=self.key_schema_string,
            schema_registry_client=self.schema_registry_client
        )
        return {'key.serializer': avro_key_serializer}

    @property
    def key_schema_string(self):
        key_schema = {}
        _value_schema = json.loads(self._value_schema_string)
        key_schema['type'] = _value_schema['type']
        key_schema['name'] = f'{_value_schema["name"]}Key'
        key_schema['namespace'] = _value_schema['namespace']
        key_schema['fields'] = self._find_key_fields(_value_schema['fields'])
        return str(key_schema).replace("'", "\"")

    @staticmethod
    def _get_schema_string(item: KafkaModel):
        return item.schema_json()

    @staticmethod
    def _results_to_dict(results: KafkaModel, _):
        return json.loads(results.json())

    def _find_key_fields(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        def is_key(field):
            return any(v == self._key for _, v in field.items())

        return [field for field in fields if is_key(field)]
