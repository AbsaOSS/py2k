from py2k.writer import KafkaWriter

writer = KafkaWriter(
    topic='dummy_topic',
    schema_registry_config={'url': 'http://myschemaregistry.com:8081'},
    producer_config={'bootstrap.servers': 'myproducer.com:9092'}
)
