import sys, datetime, time, uuid, random
from json import dumps, loads
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

class PatchedAvroProducer(AvroProducer):
    def __init__(self, config, default_key_schema=None, default_value_schema=None, schema_registry=None):
        super(PatchedAvroProducer, self).__init__(config, default_key_schema=default_key_schema,
                                                  default_value_schema=default_value_schema,
                                                  schema_registry=schema_registry)

    def produce(self, **kwargs):
        key_schema = kwargs.pop('key_schema', self._key_schema)
        value_schema = kwargs.pop('value_schema', self._value_schema)
        topic = kwargs.pop('topic', None)
        if not topic:
            raise ClientError("Topic name not specified.")
        value = kwargs.pop('value', None)
        key = kwargs.pop('key', None)

        if value is not None:
            if value_schema:
                value = self._serializer.encode_record_with_schema(topic, value_schema, value)
            else:
                raise ValueSerializerError("Avro schema required for values")

        if key is not None:
            if key_schema:
                key = self._serializer.encode_record_with_schema(topic, key_schema, key, True)
            else:
                pass

        super(AvroProducer, self).produce(topic, value, key, **kwargs)
