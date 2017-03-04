# Introduction #

This project is used to produce real time messages to kafka using our Kafkaproducer module and also to read in real time using our KafkaConsumer module.In Kafka Consumer,
we are using Spark Streaming as processing engine and Cassandra as data storage.

# Prerequisites #

* Apache Spark 1.6.3
* Apache Kafka 0.8.1.1
* JDK 1.8
* Cassandra 2.1

# Installation #

Enter the project root directory of Kafka Producer and Kafka Consumer and build using [Apache Maven] (http://maven.apache.org/):
```
mvn clean install
```

The build produces two JAR files:
* `KafkaConsumer/target/KafkaConsumer-1.0-SNAPSHOT-jar-with-dependencies.jar` - Kafka Producer Jar File.
* `KafkaProducer/target/KafkaProducer-1.0-SNAPSHOT-jar-with-dependencies.jar` - Spark Streaming Kafka Consumer Jar file.

Launch Kafka Producer
* `java -jar target/KafkaProducer-1.0-SNAPSHOT-jar-with-dependencies.jar`

Launch Spark Job
* `./bin/spark-submit --class com.example.SparkKafkaConsumer KafkaConsumer_ROOT_DIRECTORY/target/KafkaConsumer-1.0-SNAPSHOT-jar-with-dependencies.jar`

# Flow #

- Firstly we need to launch our Kafka Producer, which will write on two kafka topics concurrently for our two avro schemas.  
- After this, we need to launch our spark job,which will read from two kafka topics paralley by creating two direct streams from Kafka.  
- So our two streams will parallely keep on storing messages in cassandra.  
- One more aggregated stream is responsible to read messages from both kafka topics, so  it will then generate RDDs with first letters of firstname as key and firstname as value.
- Then reduce it by key and appending the firstnames as result
- So our reduce by key operation generate rdd with currenttimestamp,no_of_first_names_having_first_two_letters_same, all_first_names_that_jave_common_first_two_letters
- So we are saving the results into a csv file and into cassandra also.
- Our CSV results are correct but cassandra is showing some incorrect results of aggregation.We are working on this issue.
