package spark_practice.dataset.scala

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.io.Source

object create_dataset {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software\\")

    val spark = SparkSession.builder()
      .master("local[3]").appName("spark_debugg")
      .config("spark.sql.shuffle.partition","3").getOrCreate()
    import spark.implicits._

    val ds = spark.read.option("inferSchema",true).option("header",true).csv("./data/create_dataset.csv")
    val empDS : Dataset[Emp] = ds.as[Emp]

     println( empDS.repartition(2).map(_.age).collect().mkString("->") )
    empDS.foreach(e => {
      println(s" age = ${e.age} ")
    })
    //scala.io.StdIn.readLine()

    val sparkConf = new SparkConf
    val props = new Properties
    val fileName = "spark.conf"

    // reading from resources folder
    props.load(Source.fromFile("./src/main/resources/db_config.properties").bufferedReader())
    props.forEach(
      (k,v) => {
                      println(s"key is :: ${k} and value is :: ${v}")
                      sparkConf.set(k.toString,v.toString)
               }
    )

    sparkConf.getAll.foreach(e => println(e))

    //empDS.printSchema()
    //empDS.show()
    spark.stop
    println()

  }

}
case class Emp(id:Int,name:String,age:String,join_date:String)