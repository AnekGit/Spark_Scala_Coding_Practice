package spark_practice

import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object scala_timestamp {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir", "C:\\Spark_3_Software")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println(spark.sparkContext.parallelize(Seq(1, 2), 2).glom().collect())
    import spark.implicits._
    /*val dateData = Seq(
      (1, Date.valueOf("2016-09-30")),
      (2, Date.valueOf("2016-12-14"))
    )
    val dateDF : Dataset[Row] = dateData.toDF("id","join_date")

    dateDF.printSchema()
    dateDF.show()
*/
    val timeRawData = Seq(
      (1, "2016-09-30 00:00:00"),
      (2, "2016-06-01 00:00:00")
    )
    import org.apache.spark.sql.functions._
    timeRawData.toDF("id","trans_date")
      .withColumn( "trans_date_",col("trans_date").cast(DataTypes.DateType) )
      .withColumn("trans_date__",to_date(col("trans_date")))
     /* .where("trans_date = to_date('2016-06-01','yyyy-MM-dd') ")*/
      .show


    val timeRawData2 = Seq(
      (1, "20-02-2016 00:00:00"),
      (2, "21-02-2016 00:00:00")
    )
    import org.apache.spark.sql.functions._
    timeRawData2.toDF("id","trans_date")
     /* .withColumn( "trans_date_",col("trans_date").cast(DataTypes.DateType) )*/
      .withColumn("trans_date__",to_date( col("trans_date"), "dd-MM-yyyy HH:mm:ss" ) )
       .where("trans_date__ = to_date('2016-06-01','yyyy-MM-dd') ")
      .show
   /* val timeData = Seq(
      (1, java.sql.Timestamp.valueOf("2016-09-30 10:15:20")),
      (2, java.sql.Timestamp.valueOf("2016-12-14 11:19:22"))
    )
    val timestampDF : Dataset[Row] = timeData.toDF("id","join_timestamp")

    timestampDF.printSchema()
    timestampDF.show()
*/

  }
}