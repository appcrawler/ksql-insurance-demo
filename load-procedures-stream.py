import sys, datetime, time, uuid, random
from json import dumps, loads
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from PatchedAvroProducer import *

value_schema_str = """
{
  "type": "record",
  "name": "value_procedures_s",
  "namespace": "procedures_stream",
  "fields": [
    {
      "name": "procedure_id",
      "type": "string"
    },
    {
      "name": "procedure_name",
      "type": "string"
    },
    {
      "name": "fee_tolerance",
      "type": "double"
    },
    {
      "name": "frequency_tolerance",
      "type": "int"
    }
  ]
}
"""

key_schema_str = """
{
  "type": "record",
  "name": "key_procedures_s",
  "namespace": "procedures_stream",
  "fields": [
    {
      "name": "procedure_id",
      "type": "string"
    }
  ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)

"""
avroProducer = PatchedAvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=key_schema, default_value_schema=value_schema)
"""

avroProducer = PatchedAvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=None, default_value_schema=value_schema)

i=1
while i <= 100:
  i += 1
  procedure_id = "PROCEDURE" + str(i)
  s = {"procedure_id":procedure_id,
       "procedure_name":"Procedure " + str(i),
       "fee_tolerance":random.randint(200,4999),
       "frequency_tolerance":random.randint(29,364)}
  print(s)
  key = {"procedure_id":procedure_id}
  key = procedure_id
  print(key)

  avroProducer.produce(topic='procedures', key=key, value=s)
  avroProducer.flush()
