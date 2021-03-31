# History

## 1.8.0 (2021-03-29)

- Adhering to Kafka and Avro parlance by renaming:
  - models module -> record
  - KafkaModel -> KafkaRecord
  - DynamicPandasModel -> PandasToRecordsTransformer
  - item -> record
- Move schema knowledge to KafkaRecord
- Introduce `__key_fields__` in KafkaRecord to enable specifying which fields are part of the key
- Introduce `__include_key__` in KafkaRecord to enable specifying whether key_fields should be part of the value message

Big thank you to [@vesely-david](https://github.com/vesely-david) for this change

## 1.7.0 (2021-03-11)

- Minor API change for easier dynamic creation of KafkaModels from a pandas DataFrame

## 1.6.0 (2021-03-01)

- First commit on Github.
