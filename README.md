# MLE

# Kafka Server

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --partitions 2 --replication-factor 1 --topic my-topic --bootstrap-server localhost:9092
