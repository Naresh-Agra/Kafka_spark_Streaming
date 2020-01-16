package Kafka_Spark_Streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.io.Source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc._
import com.mysql.cj.jdbc.Driver
import com.databricks.spark.xml._

object Kafka_Spark_Streaming {
    def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("Spark_XML1").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession
              .builder()
              .config(conf)
              .getOrCreate()
              
    import spark.implicits._
    
    
    
    
    
    
    }
  
}