package com.example

import java.io.{ FileInputStream, FileWriter }
import java.util.Properties

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{ SomeColumns, rdd, _ }
import com.google.gson.Gson
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext, metrics }
import org.apache.spark.metrics.source
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ListBuffer
import scala.tools.nsc.interpreter
import scala.tools.nsc.interpreter.session
import com.datastax.spark.connector._
import org.apache.spark.streaming.dstream.InputDStream
/*
* This class is used to receive messages from two Kafka topics
*and it stores the messages into cassandra and also perform aggregation upon them and store their results into csv file and cassandra
* */
case class Event(event_timestamp: Long, firstname: String, lastname: String, phonenumber: Long, address: String)
object SparkKafkaConsumer extends Serializable {

  var avroSchemaVer1 = ""
  var avroSchemaVer2 = ""
  /*
  * Entry point of our application
  * */
  def main(args: Array[String]): Unit = {
    if (System.getenv("POC_SCALA_CONSUMER_CONFIG") == null) {
      println("Please set POC_SCALA_CONSUMER_CONFIG environment variable which should point to your config.properties file")
      System.exit(1)
    }
    val (topics, brokerHost, cassandraHost, database, tableEvent, tableAggEvent, csvOutput, avroSchemaPathV1, avroSchemaPathV2, sparkAppName, sparkCheckpointDir) =
      try {
        val prop = new Properties()
        // Load a properties file config.properties from project classpath, and retrieved the property value.
        prop.load(new FileInputStream(System.getenv("POC_SCALA_CONSUMER_CONFIG")))
        // retrieved the property value
        //    * @topics return topics 
        //    * @brokerHost return kafka broker server host and post no
        //    * @cassandraHost return cassandra host
        //    * @database return cassandra database name
        //    * @tableEvent return cassandra event table
        //    * @tableAggEvent  return cassandra aggregation event table
        //    * @csvOutput  return our csv file path which will store aggregation results
        //    * @avroSchemaV1 return path of avro schema v1
        //    * @avroSchemaV2 return path of avro schema v2
        //    * @sparkAppName return name of the spark app
        //    * @sparkCheckpointDir return the checkpoint directory which stores kafka offsets
        //    */
        (prop.getProperty("kafka.topics"), prop.getProperty("kafka.broker.host"), prop.getProperty("cassandra.connection.host"), prop.getProperty("cassandra.keyspace"), prop.getProperty("cassandra.table.event"), prop.getProperty("cassandra.table.event.aggregation"), prop.getProperty("aggregation.csv.outputpath"), prop.getProperty("schema.path"), prop.getProperty("schemav2.path"), prop.getProperty("spark.app.name"), prop.getProperty("spark.checkpoint.directory"))
      } catch { case e: Exception => e.printStackTrace(); sys.exit(1); }
    // creating avroschema objects from schema file    
    val schemaFileVer1 = scala.io.Source.fromFile(avroSchemaPathV1)
    avroSchemaVer1 = try schemaFileVer1.mkString finally schemaFileVer1.close()

    val schemaFileVer2 = scala.io.Source.fromFile(avroSchemaPathV2)
    avroSchemaVer2 = try schemaFileVer2.mkString finally schemaFileVer2.close()

    // starting spark streaming context
    val ssc = StreamingContext.getOrCreate(sparkCheckpointDir, () => createContext(topics, brokerHost, cassandraHost, database, tableEvent, tableAggEvent, csvOutput, sparkAppName, sparkCheckpointDir)); ssc.start(); ssc.awaitTermination(); ssc.stop(true, true);
  }
  def createContext(topics: String, brokerHost: String, cassandraHost: String, database: String, tableEvent: String, tableAggEvent: String, csvOutput: String, sparkAppName: String, sparkCheckpointDir: String): StreamingContext = {
    //  SparkConf object that contains information about your application
    val conf: SparkConf = new SparkConf().setAppName(sparkAppName).setMaster("local[1]")
	.set("spark.cassandra.connection.host", cassandraHost)
    //  The maximum total size of the batch in bytes.
    conf.set("spark.cassandra.output.batch.size.rows", "auto")
    // maximum concurrent writes in a spark job
    conf.set("spark.cassandra.output.concurrent.writes", "10")
    // Number of bytes per single batch
    conf.set("spark.cassandra.output.batch.size.bytes", "100000")
    // writing headers to our csv results file
    writeHeaders(csvOutput)
    /// create  a local StreamingContext with batch interval of 1 second
    val sc = new SparkContext(conf)
    // adding a interval of 5 seconds
    val ssc = new StreamingContext(sc, Seconds(5));
    ssc.remember(Seconds(60 * 2));
    ssc.checkpoint(sparkCheckpointDir);

    // two kafka direct stream to read the data from two topics in parallel by spark job
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, Map[String, String]("metadata.broker.list" -> brokerHost), topics.split(",").toSet)

    // simply reading from kafka stream version
    var dStream = kafkaStream.map {
      case (_, msg) =>
        // getting avro record on the basis of schema of msg & parsing it
       parseMessage(msg)
    }
    saveToCassandra(dStream, database, tableEvent);
    saveAggregatesToCassandra(kafkaStream, database, tableAggEvent, csvOutput);

    ssc
  }
  def saveToCassandra(dStream: DStream[Event], database: String, tableEvent: String): Unit = {
    dStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        rdd.saveToCassandra(database, tableEvent, SomeColumns("event_timestamp", "firstname", "lastname", "phonenumber", "address"))
    })
  }
  def saveAggregatesToCassandra(kafkaStream: InputDStream[(String, Array[Byte])], database: String, tableAggEvent: String, csvOutputPath: String): Unit = {
    kafkaStream.map {
      case (_, msg) =>
        // getting avrorecord from msg
        val record: GenericRecord = getAvroSchema(msg)
        val firstname: String = record.get("firstname").toString
        // generating rdd key as first two letters of firstname , value as firstname
        (firstname.substring(0, 2): String, firstname: String)
      // reducing by key and appending the names
    }.reduceByKey((namesList, name) => namesList + "," + name)
      .map {
        case (firstNameSubstring, firstNames) =>
          // getting the number of firstnames match with any particular key i.e. first two letters of first name
          var count: Long = firstNames.split(",").length
          // writing to our csv file
          writeToCsv(count, firstNames, csvOutputPath)
          // creating rdd timestamp,count,all_first_names that matched
          ((System.currentTimeMillis / 1000), count, firstNames)
      }.foreachRDD(rdd =>
        // aggregation results stored to cassandra
        rdd.saveToCassandra(database, tableAggEvent, SomeColumns("event_timestamp", "count", "names")))
  }
/*
  * used to get avro record from kafka msg
  * @param  msg:Array[Byte]
  * @return GenericRecord
  * */
  def getAvroSchema(msg: Array[Byte]):GenericRecord={
   try
     GenericAvroCodecs.toBinary(new Schema.Parser().parse(avroSchemaVer1)).invert(msg).get
   catch {
      case e: Exception =>{
		GenericAvroCodecs.toBinary(new Schema.Parser().parse(avroSchemaVer2)).invert(msg).get
		}
	}
  }
  /*
  * used to generate event from kafka msg
  * @param  msg:Array[Byte]
  * @return Event
  * */
  def parseMessage(msg: Array[Byte]): Event = {
    try{
      processMessage(false,GenericAvroCodecs.toBinary(new Schema.Parser().parse(avroSchemaVer1)).invert(msg).get)
    }catch {
      case e: Exception => {
        processMessage(true,GenericAvroCodecs.toBinary(new Schema.Parser().parse(avroSchemaVer2)).invert(msg).get)
      }
    }
  }
   /*
  * used to process message to check versions
  * @param  hasAddress:Boolean
  * @param  msg:Array[Byte]
  * @return Event
  * */
  def processMessage(hasAddress:Boolean,record: GenericRecord):Event={
     var address:String="N/A"
     if(hasAddress)
       address=record.get("address").toString
     Event(System.currentTimeMillis / 1000, record.get("firstname").toString, record.get("lastname").toString, record.get("phonenumber").toString.toLong,address)
  }	
  

  /*
  * method used to write headers to our output csv file
  * */
  def writeHeaders(filePath: String): Unit = {
    val fw = new FileWriter(filePath, true)
    try fw.write("Count,Names" + "\n") finally fw.close()
  }

  /*
  * method used to write aggregated results to our output csv file
  * */
  def writeToCsv(count: Long, names: String, filePath: String): Unit = {
    val fw = new FileWriter(filePath, true)
    try fw.write(count + "," + names + "\n") finally fw.close()
  }
}
