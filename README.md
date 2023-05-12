# Spark Structured Streaming Using Kafka 

### Cryptocurrency analysis through spark by sending the data to kafka and reading it through spark and writing it to console

The repositery consists of one producer file and 5 consumer files each has a specific functionality.


I used spark version: 3.3.2 
python version : 3.8 
since the kafka package don't support versions after 3.8 at the moment

to run the files we need to 
1- run zookeeper by 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2- run the kafka server 
```
bin/kafka-server-start.sh config/server.properties
```
3- run the producer file 
```
python crypto_producer_stream.py
```
4- run one of the consumer files through spark while adding the jar file
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 groupby.py
```

Now you will see the results printed in the console once it reads data from kafka
