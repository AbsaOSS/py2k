# Py2k

A high level Python to Kafka API with Schema Registry compatibility and automatic avro schema creation.

- Free software: Apache2 license

## Installation

Py2K is currently available on PIP:

```bash
 > pip install py2k
```

## Contributing

Please see the [Contribution Guide](.github/CONTRIBUTING.md) for more information.

## Usage

### Minimal Example

```python
from py2k.models import PandasToKafkaTransformer
from py2k.writer import KafkaWriter

# assuming we have a pandas DataFrame, df
records = PandasToKafkaTransformer(df=df, record_name='test_model').from_pandas()

writer = KafkaWriter(
    topic="topic_name",
    schema_registry_config=schema_registry_config,
    producer_config=producer_config
)

writer.write(records)
```

## Features

- Schema Registry Integration
- Automatic Avro Serialization from pandas DataFrames
- Automatic Avro Schema generation from pandas DataFrames and Pydantic objects

## License

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
