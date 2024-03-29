# Release Notes

<!-- prettier-ignore-start -->

## Latest Changes



<!-- prettier-ignore-end -->

## v1.9.1 (2021-10-04)

### Bug

- 🐛 Kafka Schema was not extracted from entire Dataframe. [#76](https://github.com/AbsaOSS/py2k/pull/76) - [@vesely-david](https://github.com/vesely-david)

## v1.9.0 (2021-05-15)

### Enhancement

- ✨ Support for dynamic creation to return an iterator. [#67](https://github.com/AbsaOSS/py2k/pull/67) - [@vesely-david](https://github.com/vesely-david)

  - User can choose whether to return list (from_pandas) or iterator (iter_from_pandas)
  - KafkaWriter was adjusted accordingly to accept any iterable in it's write method

## v1.8.2 (2021-04-06)

### Bugs

- Resolved boolean schema not being converted to the correct avro schema values PR [#48](https://github.com/AbsaOSS/py2k/pull/48) - [@vesely-david](https://github.com/vesely-david)

## v1.8.1 (2021-03-31)

### Docs

- Added examples and solved mkdocs gitub.io page build - [@DanWertheimer](https://github.com/DanWertheimer). PR [#45](https://github.com/AbsaOSS/py2k/pull/45)

## v1.8.0 (2021-03-29)

### Fixes

- Adhering to Kafka and Avro parlance by renaming:
  - models module -> record
  - KafkaModel -> KafkaRecord
  - DynamicPandasModel -> PandasToRecordsTransformer
  - item -> record
- Move schema knowledge to KafkaRecord
- Introduce `__key_fields__` in KafkaRecord to enable specifying which fields are part of the key
- Introduce `__include_key__` in KafkaRecord to enable specifying whether key_fields should be part of the value message

Big thank you to [@vesely-david](https://github.com/vesely-david) for this change

## v1.7.0 (2021-03-11)

- Minor API change for easier dynamic creation of KafkaModels from a pandas DataFrame

## v1.6.0 (2021-03-01)

- First commit on Github.
