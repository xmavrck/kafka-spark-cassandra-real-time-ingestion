package com.example.kafkaproducer

import java.io.FileInputStream

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

/*
 * This class is acting as Kafka Producer in which we run two threads concurrently
 * to write avro messages to Kafka Queue.
 * */

object MyKafkaProducer {

  val VERSION_1 = 1
  val VERSION_2 = 2

  def main(args: Array[String]): Unit = {
    if (System.getenv("POC_SCALA_PRODUCER_CONFIG") == null) {
      println("Please set POC_SCALA_PRODUCER_CONFIG environment variable which should point to your config.properties file")
      System.exit(1)
    }
    /*
     * @topicV1 return topic for version 1 avro message
     * @topicV2 return topic for version 2 avro message
     * @brokerHost return kafka broker server host and post no
     * @filePathV1 file path to csv for v1 firstname
     * @filePathV2 file path to csv for v2 firstname
     */
    val (topicV1, topicV2, brokerHost, filePathV1, filePathV2, avroSchemaPathV1, avroSchemaPathV2) =
      try {
        val prop = new Properties()

        // Load a properties file config.properties from project classpath, and retrieved the property value.
        prop.load(new FileInputStream(System.getenv("POC_SCALA_PRODUCER_CONFIG")))
        //   retrieved the property value.
        (prop.getProperty("kafka.topic.version1"), prop.getProperty("kafka.topic.version2"), prop.getProperty("kafka.broker.host"),
          prop.getProperty("sample.firstname.filepath.v1"), prop.getProperty("sample.firstname.filepath.v2"),
          prop.getProperty("schema.path"), prop.getProperty("schemav2.path"))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
      }
    // kafka config properties
    val props: Properties = new Properties()
    // your kafka broker server host and port no, need to be set in your environment variable
    props.put("bootstrap.servers", brokerHost)
    // key serializer class
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //this property we used in case of huge incoming requests,so we artificially delay the request for 5ms.
    // So by default batch.size is 16384.So if batch size is reached before 5 ms, then it will send to kafka by our producer.
    // but if our batch size is not met,then producer will wait for 5 ms and then sent to Kafka Broker.
    props.put("linger.ms", "5")
    // using this compression as its tranfer rate is high and also it takes less time to decompress
    // props.put("compression.type", "lz4")
    // to maintain messages ordering, because if messages batch failed , and on retry by kafka producer,the failed messages will be sent in second batch
    props.put("max.in.flight.requests.per.connection", "1")
    // avro messages are sent in byte[], so we are using ByteArraySerializer
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    //launching our kafka producer for avro schema version 1
    new Thread {
      override def run {
        userInfo(props, avroSchemaPathV1, topicV1, filePathV1, VERSION_1)
      }
    }.start()
    // launching our kafka producer for avro schema version 2
    new Thread {
      override def run {
        userInfo(props, avroSchemaPathV2, topicV2, filePathV2, VERSION_2)
      }
    }.start()

    /*
    * this method used to send messages to our first kafka topic
    * @param props Kafka broker properties
    * */
    def userInfo(props: Properties, avroSchemaPath: String, topic: String, filePath: String, version: Integer): Unit = {
      //  schema can be instantiated
      val schemaFile = scala.io.Source.fromFile(avroSchemaPath)
      val avroSchema = try schemaFile.mkString finally schemaFile.close()
      
      val parser: Schema.Parser = new Schema.Parser()
      //  USER_SCHEMA_V2 is json as described above
      val schema: Schema = parser.parse(avroSchema)
      // kafka producer
      val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)
      var count = 0;
      while (true) {
        // read .csv file and path to .csv file that contain first name
        val bufferedSource = io.Source.fromFile(filePath)
        for (line <- bufferedSource.getLines) {
          // read record by line and split by , (comma) operator
          val cols = line.split(",").map(_.trim)
          // getting the avro record according to version number and converting the avro record into bytes and passing it into producerrecord and it will send by our kafka producer
          producer.send(new ProducerRecord[String, Array[Byte]](topic, GenericAvroCodecs.toBinary(schema).apply(getAvroRecord(cols(0), schema, version))))
          //  genrate  1000 events per sec
          if (count == 1000) {
            Thread.sleep(1000);
            count = 0;
          }
          count = count + 1;
        }
        bufferedSource.close
      }
      // close the kafka producer
      producer.close()
    }
    /*
    * used to generate avro record dynamically using version number
    * @param firstName
    * @param schema
    * @param version number
    * @return GenericData.Record
    * */
    def getAvroRecord(firstName: String, schema: Schema, version: Integer): GenericData.Record = {
      val avroRecord: GenericData.Record = new GenericData.Record(schema)
      avroRecord.put("firstname", firstName)
      // put last name
      avroRecord.put("lastname", "Singh")
      // put phone number
      avroRecord.put("phonenumber", 9876852789L)
      // put address
      if (version == 2) {
        avroRecord.put("address", "Chandigarh")
      }
      avroRecord
    }

  }
}
