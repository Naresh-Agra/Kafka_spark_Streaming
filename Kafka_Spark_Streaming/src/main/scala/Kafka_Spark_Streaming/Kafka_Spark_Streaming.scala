package Kafka_Spark_Streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._

object Kafka_Spark_Streaming {
    def main(args:Array[String]):Unit={
      
    println("Spark Execution Started")
    val conf=new SparkConf().setAppName("Spark_XML1").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession
              .builder()
              .config(conf)
              .getOrCreate()
              
    import spark.implicits._
   
    val kafkaParams = Map[String, Object](
                    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
                    "key.deserializer" -> classOf[StringDeserializer],
                    "value.deserializer" -> classOf[StringDeserializer],
                    "group.id" -> "stream_id",
                    "auto.offset.reset" -> "latest"
                    )
    
    val ssc = new StreamingContext(conf,Seconds(2))
    val topics = Array("kafkastruct01")
    val stream = KafkaUtils.createDirectStream[String, String](
                  ssc,
                  PreferConsistent,
                  Subscribe[String, String](topics, kafkaParams)
                  )

    val dstreams=stream.map(x => x.value)
    dstreams.print()
    dstreams.foreachRDD(x=>
    if(!x.isEmpty())
  {

    val flattendata=x.flatMap(x=>x.split(","))
    flattendata.foreach(println)

}
)
    ssc.start()
    ssc.awaitTermination()

    
    }
  
}