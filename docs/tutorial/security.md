# Security

Py2K is built on top of the [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) python package which uses [librdkafka](https://github.com/edenhill/librdkafka) configuration for authentication.

## SSL Authentication Example

SSL authentication is built into the base Py2K wheels.

As an example, the below config and setup will work:

```Python
{!../docs_src/security/security001.py!}
```

## SASL_SSL Kerberos Authentication Example

<!-- prettier-ignore-start -->
!!! info
    The Py2K installation install confluent-kafka for you, **however** the base confluent-kafka librdkafka linux wheel is not built with SASL Kerberos/GSSAPI support and if you required this you will need to install the wheels on your system first. For a guide, see [here](https://github.com/confluentinc/confluent-kafka-python#prerequisites)
<!-- prettier-ignore-end -->

Once you've built from source you can use a similar base config to below:

```Python
{!../docs_src/security/security002.py!}
```
