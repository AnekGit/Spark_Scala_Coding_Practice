package guru.learningjournal.spark.examples

import java.util
import java.util.Properties

import scala.language.implicitConversions
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, udf}

import scala.io.Source

object HelloSpark extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }
    logger.info(Runtime.getRuntime.availableProcessors());

    logger.info("Starting Hello Spark")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()
    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    val surveyRawDF = loadSurveyDF(spark, args(0))
                              
    val collectingDF = surveyRawDF.select("Country","state","Gender")
       .withColumn("country_state",concat(col("Country"),lit("->"),col("state")))

    val partitionedSurveyDF = surveyRawDF.repartition(2)
   
                         
    val countDF = countByCountry(partitionedSurveyDF)
    countDF.foreach(row => {
      logger.info("Country: " + row.getString(0) + " Count: " + row.getLong(1))
    })

    //logger.info(countDF.collect().mkString("->"))

    logger.info("Finished Hello Spark")
    //scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/
    println( spark.sparkContext.parallelize(
      Seq("Anek","2020/05/05")
    ).collect() )
    spark.stop()
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    //This is a fix for Scala 2.11
    //import scala.collection.JavaConverters._
    //props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
    sparkAppConf
  }

}
