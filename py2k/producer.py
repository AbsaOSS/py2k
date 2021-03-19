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

from confluent_kafka import SerializingProducer
from confluent_kafka import KafkaError, Message


class KafkaProducer:
    def __init__(self, topic, producer_config):
        self._topic = topic
        self._producer = SerializingProducer(producer_config.dict)
        self._serializer = producer_config.serializer

    def produce(self, item):
        while True:
            try:
                key, value = self._serializer.serialize(item)

                self._producer.produce(
                    topic=self._topic,
                    key=key,
                    value=value,
                    on_delivery=self._delivery_report
                )
                self._producer.poll(0)
                break
            except BufferError as e:
                print(
                    f'Failed to send on attempt {item}. '
                    f'Error received {str(e)}')
                self._producer.poll(1)

    def flush(self):
        if self._producer:
            self._producer.flush()

    @staticmethod
    def _delivery_report(err: KafkaError, msg: Message):
        """ Reports the failure or success of a message delivery.

        Note:
            In the delivery report callback the Message.key()
            and Message.value() will be the binary format as
            encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s)
            for key and value to delivery
            report callback we recommend a bound callback
            or lambda where you pass the objects along.

        Args:
            err ([KafkaError]): The error that occurred on None on success.
            msg ([Message]): The message that was produced or failed.
        """

        if err is not None:
            print(
                f"Delivery failed for record {msg.key()}: {err}"
            )
