import json
from copy import deepcopy
from typing import Union, List, Dict, Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from py2k.models import KafkaModel


class ProducerConfig:
    def __init__(self, key, default_config, schema_registry_config, data):
        self._key = key
        self._default_config = default_config
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        self._data = data
        self._value_schema_string = self._get_schema_string(self._data)
        self._config_build = None

    def get(self):
        if self._config_build:
            return self._config_build

        self._config_build = deepcopy(self._default_config)
        self._create_value_serializer()
        self._create_key_serializer()

    def _create_value_serializer(self):
        avro_value_serializer = AvroSerializer(
            self._value_schema_string,
            self.schema_registry_client,
            to_dict=self._results_to_dict)
        self._config_build.update({
            'value.serializer': avro_value_serializer
        })

    def _create_key_serializer(self):
        if self._key:
            avro_key_serializer = AvroSerializer(
                schema_str=self.key_schema_string,
                schema_registry_client=self.schema_registry_client
            )
            self._config_build.update({
                'key.serializer': avro_key_serializer
            })

    @property
    def key_schema_string(self):
        key_schema = dict()
        _value_schema = json.loads(self._value_schema_string)
        key_schema['type'] = _value_schema['type']
        key_schema['name'] = f'{_value_schema["name"]}Key'
        key_schema['namespace'] = _value_schema['namespace']
        key_schema['fields'] = self._resolve_key_fields_(
            _value_schema['fields'], self._key
        )
        print(key_schema)
        return str(key_schema).replace("'", "\"")

    @staticmethod
    def _get_schema_string(results: Union[List[KafkaModel], KafkaModel]):
        return results[0].schema_json()

    @staticmethod
    def _results_to_dict(results: KafkaModel, _):
        return json.loads(results.json())

    @staticmethod
    def _resolve_key_fields_(fields: List[Dict[str, Any]],
                             key: str) -> Dict[str, Any]:
        final = list()
        for field in fields:
            for _, v in field.items():
                if v == key:
                    final.append(field)
        return final