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
# create topics for orders, customers, and pizze status, as well as predefined topics
# to hold our KSQL streams and tables

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
# sleep 20 seconds to ensure everything is up, then create connectors for our source
# TIBCO queues, as well as our GCP BigQuery sink
sleep 20

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @/root/claims-sink.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @/root/procedures-sink.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @/root/customers-sink.json
#---------------------------------------------------------------------------------
# clean up metadata for our TIBCO producer, then run TIBCO and KSQL DDL
python load-customers-stream.py
python load-claims-stream.py 100
python load-providers-stream.py
python load-procedures-stream.py
ps -ef | grep tibemsd | grep -v grep
if [ $? -ne 0 ]; then
  d=$(pwd)
  cd /tmp 
  nohup tibemsd &
  cd $d
else
  ps -ef | grep tibemsd
fi
echo "RUN SCRIPT '/root/anthem.ksql';" | ksql
#---------------------------------------------------------------------------------
