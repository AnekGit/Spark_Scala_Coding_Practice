package spark_practice

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

object ScalaMetaDataBuilder {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir", "C:\\Spark_3_Software")
      .config("spark.sql.shuffle.partitions","3")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val data = Seq((12, 20180411), (5, 20180411), (11, 20180411), (2, 20180412), (29, 20180413),
      (31, 20180414), (18, 20180415), (2, 20180412), (31, 20180413), (8, 20180416), (29, 20180413),
      (31, 20180414), (8, 20180415), (2, 20180412), (23, 20180413), (51, 20180414), (15, 20180415))

    import spark.implicits._
    var orders : Dataset[Row] = data.toDF("order_id", "date")
  //  orders = orders.withColumn("date",to_date(col("date").cast(DataTypes.StringType),"yyyyMMdd") )
                        orders.printSchema
                        orders.show
    val dsMax :Dataset[Int]= orders.agg( max( col("date") ).as("max_date") ).as[Int]
    println("Dataset[Int] => ds.show :: ")
    dsMax.show()

    println("Dataset[Int] => ds.javaRDD.collect :: ")
    val i :Int  = dsMax.javaRDD.collect.get(0)
    println( i )


    orders.agg( max( col("date") ).as("max_date") ).as[Int].show
    println( orders.agg( max( col("date") ).as("max_date") ).as[Int].take(1) )
    //val maxDateString : String = maxDate.collect()(0)
   // print(maxDateString)

    val metadata: Metadata = new MetadataBuilder().putLong("max_dt",i).build()
    val dsGroup = orders.groupBy('date as("date", metadata)).agg(count("order_id") as "order_count")
    dsGroup.show()

      println( "dsGroup.schema(\"date\") "+dsGroup.schema("date"))
  
      dsGroup.schema("date").metadata.getLong("max_dt")

    import java.sql.Timestamp
    import spark.implicits._

    val df = Seq(
      (1, Timestamp.valueOf("2014-01-01 23:00:01")),
      (1, Timestamp.valueOf("2014-11-30 12:40:32")),
      (2, Timestamp.valueOf("2016-12-29 09:54:00")),
      (2, Timestamp.valueOf("2016-05-09 10:12:43"))
    ).toDF("typeId","eventTime")

    df.printSchema()
    df.show()

  }
}