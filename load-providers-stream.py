import sys, datetime, time, uuid, random
from json import dumps, loads
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from PatchedAvroProducer import *

value_schema_str = """
{
  "type": "record",
  "name": "value_providers_s",
  "namespace": "providers_stream",
  "fields": [
    {
      "name": "provider_id",
      "type": "string"
    },
    {
      "name": "provider_name",
      "type": "string"
    },
    {
      "name": "provider_status",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    },
    {
      "name": "city",
      "type": "string"
    },
    {
      "name": "state",
      "type": "string"
    }
  ]
}
"""

key_schema_str = """
{
  "type": "record",
  "name": "key_providers_s",
  "namespace": "providers_stream",
  "fields": [
    {
      "name": "provider_id",
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
while i <= 300:
  i += 1
  provider_id = "PROVIDER" + str(i)
  s = {"provider_id":provider_id,
       "provider_name":"Health Care Provider " + str(i),
       "provider_status":"ACTIVE",
       "address":str(random.randint(1,10000)) + " " + random.choice(['Broad St.','Plum Ave.','Waggoner Rd.']),
       "city":random.choice(['Niceburg','Pleasantville','Happy Town']),
       "state":random.choice(['Ohio','Michigan','Indiana','Kentucky'])}
  print(s)
  key = {"provider_id":provider_id}
  key = provider_id
  print(key)

  avroProducer.produce(topic='providers', key=key, value=s)
  avroProducer.flush()
