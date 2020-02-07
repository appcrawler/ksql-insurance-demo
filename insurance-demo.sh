#---------------------------------------------------------------------------------
# Stop anything currently running on this host and delete for fresh start
confluent local stop
rm -rf /tmp/confluent.*
rm -rf /var/lib/kafka
#---------------------------------------------------------------------------------

#---------------------------------------------------------------------------------
# sleep five seconds to ensure everything to eliminate startup errors
sleep 5
confluent local start
#---------------------------------------------------------------------------------

#---------------------------------------------------------------------------------
# create topics

kafka-topics --bootstrap-server localhost:9092 --create --topic customers --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic claims --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic payments --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic providers --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic procedures --partitions 1 --replication-factor 1
#---------------------------------------------------------------------------------

#clean up MySQL
echo "drop table claims;" | mysql -u root --database=test
echo "drop table procedures;" | mysql -u root --database=test
echo "drop table customers;" | mysql -u root --database=test

#---------------------------------------------------------------------------------
# sleep 20 seconds to ensure everything is up
sleep 20

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @claims-sink.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @procedures-sink.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @customers-sink.json
#---------------------------------------------------------------------------------
#load initial events
python load-customers-stream.py
python load-claims-stream.py 100
python load-providers-stream.py
python load-procedures-stream.py
#run KSQL stream creations
echo "RUN SCRIPT 'insurance-demo.ksql';" | ksql
#---------------------------------------------------------------------------------
