# MLE


# Commandes pour lancer le broker Kafka


# Creer le zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties 

# Lancer le broker
bin/kafka-server-start.sh config/server.properties

# Ajoute le topic my-topic
bin/kafka-topics.sh --create --partitions 2 --replication-factor 1 --topic my-topic --bootstrap-server localhost:9092


# Producer / Client

# Dans le notebook producer on peut voir les données par un client
# Pour lancer un client on utilisera plutot le scipt client.py car on peut en lancer plusieurs


# Consumer

# Dans le notebook consumer on récupère les données des clients, toutes les 10 images on écrit les données dans le fichier data


# DataFrame

# Dans le notebook dataframe on récupère les données du fichier data, on crée un dataframe pyspark et on effecture des traitements dessus
