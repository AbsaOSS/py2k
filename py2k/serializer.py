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
                key = self._key_object(item)

                self._producer.produce(
                    topic=self._topic,
                    key=key,
                    value=item,
                    on_delivery=self._delivery_report
                )
                self._producer.poll(0)
                break
            except BufferError as e:
                print(
                    f'Failed to send on attempt {key}. '
                    f'Error received {str(e)}')
                self._producer.poll(1)

    def flush(self):
        if self._producer:
            self._producer.flush()

    def _key_object(self, item):
        if self._key:
            return {self._key: item.dict()[self._key]}
        else:
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
