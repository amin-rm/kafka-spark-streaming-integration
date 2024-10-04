# kafka-spark-streaming-integration



There are two Maven Project:
1. kafka  
2. Spark  

One input file is there tet.csv   

Before starting executing these code, you have should have already:
1. Kafka server running  
2. Kafka Topic (demo) shoud be created.  
3. kafka producer and consumer console started for testing purpose.





----- Some Useful Kafka Commands --------<br/>
Start Zookeeper<br/>
zookeeper-server-start.sh config\zookeeper.properties<br/>
<br/>
Start Kafka Broker<br/>
kafka-server-start.sh config/server.properties<br/>
<br/>
Create topic<br/>
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test<br/>
Newer versions(2.2+) of Kafka no longer requires ZooKeeper connection string
./kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 <br>
<br/>
List topic<br/>
kafka-topics.sh --list --zookeeper localhost:2181<br/>
 ./kafka-topics.sh --list --bootstrap-server localhost:9092<br>
<br/>
Start Producer<br/>
kafka-console-producer.sh --broker-list localhost:9092 --topic test<br/>
<br/>
Send message<br/>
How are you<br/>
Binod Suman Academy<br/>
<br/>
Receive message<br/>
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning<br/>
How are you<br/>
Binod Suman Academy<br/>


