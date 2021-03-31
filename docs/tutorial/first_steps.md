# First Steps

## KafkaRecord

The simplest KafkaRecord can be defined through inheritance of the `KafkaRecord` class:

```Python
{!../docs_src/first_steps/tutorial001.py!}
```

The `KafkaRecord` makes use of [Pydantic](https://pydantic-docs.helpmanual.io/) models to allow for automatic avro schema generation.

## KafkaWriter

The `KafkaWriter` is the object responsible for take a list of `KafkaRecords` and posting them onto Kafka. Using the `KafkaWriter` only requires that you define your schema registry config and producer config that is compatible with the [confluent_kafka API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#).
Specifically:

- [SchemaRegistryClient](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#schemaregistryclient)
- [SerializingProducer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer)

<!-- prettier-ignore-start -->
!!! note
    Additionally, you can have a look at the [**librdkafka** documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for addtional configuration options or the [**Confluent Schema Registry** documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
<!-- prettier-ignore-end -->

The `KafkaWriter` can be used as follows:

```Python
{!../docs_src/first_steps/tutorial002.py!}
```

In practice, copy this and put it into a file called `test_py2k.py` and fill in your kafka configuration.

```Python
{!../docs_src/first_steps/tutorial001.py!}

record = MyRecord(**{'name': 'Dan',
                     'age': 27,
                     'birthday': datetime.date(1993,9,4)})

{!../docs_src/first_steps/tutorial002.py!}

writer.write([record])
```

running this:

<div class="termy">

```console
$ python test_py2k.py
100%|██████████| 1/1 [00:00<00:00,  7.69it/s]

// It has now pushed that record onto Kafka
```

</div>
