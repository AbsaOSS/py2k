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


from copy import deepcopy

from py2k.serializer import KafkaSerializer


class ProducerConfig:
    def __init__(self, key, default_config, schema_registry_config, item):
        self._key = key
        self._default_config = default_config
        self._config_build = None

        self._serializer = KafkaSerializer(item,
                                           key,
                                           schema_registry_config)

    def get(self):
        if self._config_build:
            return self._config_build

        config_build = deepcopy(self._default_config)
        config_build['value.serializer'] = self._serializer.value_serializer()

        if self._key:
            config_build['key.serializer'] = self._serializer.key_serializer()

        self._config_build = config_build
        return self._config_build
