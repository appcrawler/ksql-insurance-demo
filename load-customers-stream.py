import sys, datetime, time, uuid, random
from json import dumps, loads
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from PatchedAvroProducer import *

value_schema_str = """
{
  "type": "record",
  "name": "value_customers_s",
  "namespace": "customers_stream",
  "fields": [
    {
      "name": "cust_id",
      "type": "string"
    },
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int"
    },
    {
      "name": "gender",
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
  "name": "key_customers_s",
  "namespace": "customers_stream",
  "fields": [
    {
      "name": "cust_id",
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

i=0
male_first_names = ['Michael', 'Jim', 'Stanley', 'Kevin', 'Andy', 'Dwight', 'Ryan']
female_first_names = ['Pam', 'Phyllis', 'Angela', 'Meredith', 'Kelly']
last_names = ['Scott','Halpert','Beasley','Hudson', 'Malone', 'Vance', 'Bernard', 'Martin', 'Schrute', 'Palmer', 'Howard', 'Kapoor']
genders = ['Male','Female']
states = ['MI','OH','IN','KY']
while i <= 100:
  i += 1
  cust_id = "CUST" + str(i)
  gender = genders[random.randint(0,len(genders)-1)]
  first_name = male_first_names[random.randint(0,len(male_first_names)-1)] if gender == 'Male' else female_first_names[random.randint(0,len(female_first_names)-1)]
  last_name = last_names[random.randint(0,len(last_names)-1)]
  s = {"cust_id":cust_id,
       "first_name":first_name,
       "last_name":last_name,
       "age":random.randint(0,99),
       "gender":gender,
       "state":states[random.randint(0,len(states)-1)]}
  print(s)
  key = {"cust_id":cust_id}
  key = cust_id
  print(key)

  #avroProducer.produce(topic='customers_stream', key=key, value=s)
  avroProducer.produce(topic='customers', key=key, value=s)
  avroProducer.flush()
