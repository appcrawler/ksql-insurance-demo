import sys, datetime, time, uuid, random
from json import dumps, loads
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from PatchedAvroProducer import *

value_schema_str = """
{
  "type": "record",
  "name": "value_claims_s",
  "namespace": "claims_stream",
  "fields": [
    {
      "name": "claim_id",
      "type": "string"
    },
    {
      "name": "cust_id",
      "type": "string"
    },
    {
      "name": "procedure_id",
      "type": "string"
    },
    {
      "name": "provider_id",
      "type": "string"
    },
    {
      "name": "service_time",
      "type": "long"
    },
    {
      "name": "amt",
      "type": "double"
    }
  ]
}
"""

key_schema_str = """
{
  "type": "record",
  "name": "key_claims_s",
  "namespace": "claims_stream",
  "fields": [
    {
      "name": "claim_id",
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
while i <= int(sys.argv[1]):
  i += 1
  claim_id = "CLAIM" + str(i)
  cust_id = "CUST" + str(random.randint(0,99))
  s = {"claim_id":claim_id,
       "cust_id":cust_id,
       "procedure_id":"PROCEDURE" + str(random.randint(0,99)),
       "provider_id":"PROVIDER" + str(random.randint(0,49)),
       "service_time":int(round(time.time() + random.randint(0,365),0)),
       "amt":round(random.random() * 1000,2)}
  print(s)
  key = {"claim_id":claim_id}
  key = claim_id
  print(key)

  avroProducer.produce(topic='claims', key=key, value=s)
  avroProducer.flush()
