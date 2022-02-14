package spark_practice

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source
import scala.language.implicitConversions

object RepartitionEx extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Hello Spark")
    val spark = SparkSession.builder()
      .master("local[6]").appName("RepartitionEx")
      .getOrCreate()
    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    val df = spark.read
      .option("header","true")
      .option("path","data/persons.csv")
      .option("inferSchema","true")
      .format("csv")
      .load()
    println("After reading : "+df.rdd.getNumPartitions)

    val option = Map("url"->"")
    



    /*val df_repartition = df.repartition(6,col("person_country"))
    println("repartition :"+df_repartition.rdd.getNumPartitions)
    //df_repartition.write.mode(SaveMode.Overwrite).parquet("./src/main/store/")

    val df_read = spark.read.parquet("./src/main/store/")
    println(
    df_read.rdd.getNumPartitions
    )
   df_read.count
  val  a = spark.sparkContext.parallelize(1 to 10, 5)
    print(a.collect().foreach(print))

   println( spark.sparkContext.parallelize(
      Seq("Anek","2020/05/05")
    ).collect() )*/

      spark.stop
  }
}