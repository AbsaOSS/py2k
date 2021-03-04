from uuid import uuid4

from confluent_kafka import SerializingProducer


class KafkaSerializer:
    def __init__(self, topic, key, producer_config):
        self._topic = topic
        self._key = key
        self._producer = SerializingProducer(producer_config.get())

    def produce(self, item):
        while True:
            try:
                key = self._serialize_key(item)

                self._producer.produce(
                    topic=self._topic,
                    key=self._serialize_key(item),
                    value=item,
                    on_delivery=self._delivery_report
                )
                self._producer.poll(0)
                break
            except BufferError as e:
                print(
                    f'Failed to send on attempt {key}. Error received {str(e)}')
                self._producer.poll(1)

    def _serialize_key(self, item):
        if self._key:
            return {self._key: item.dict()[self._key]}
        else:
            return self._assign_key()

    def _assign_key(self):
        if not self._key:
            return str(uuid4())

    @staticmethod
    def _delivery_report(err, msg):
        """
        Reports the failure or success of a message delivery.

        Note:
            In the delivery report callback the Message.key()
            and Message.value() will be the binary format as
            encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s)
            for key and value to delivery
            report callback we recommend a bound callback
            or lambda where you pass the objects along.

        :param err: The error that occurred on None on success.
        :type err: KafkaError
        :param msg: The message that was produced or failed.
        :type msg: Message
        """
        if err is not None:
            print(
                "Delivery failed for record {}: {}".format(msg.key(), err)
            )
