from py2k.writer import KafkaWriter

cert_config = {
    'ssl.ca.location': '/path/to/ca.pem',
    'ssl.certificate.location': '/path/to/cert.pem',
    'ssl.key.location': '/path/to/ssl.key',
}

topic = 'mytopic'
schema_registry_config = {'url': 'https://schemaregistry.com', **cert_config}
producer_config = {
    'bootstrap.servers': 'bootstrapservers.com',
    'security.protocol': 'ssl',
    **cert_config,
}

writer = KafkaWriter(
    topic=topic,
    schema_registry_config=schema_registry_config,
    producer_config=producer_config,
)
