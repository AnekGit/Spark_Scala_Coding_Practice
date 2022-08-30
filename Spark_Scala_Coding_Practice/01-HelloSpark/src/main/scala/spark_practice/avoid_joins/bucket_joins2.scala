package spark_practice.avoid_joins

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM
 */
object bucket_joins2 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val list = List(1,2,3)
    println(list)
    //-Dlog4j.configuration=file:log4j.properties -Dlogfile.name=bucket-spark -Dspark.yarn.app.container.log.dir=app-log

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    System.setProperty("spark.sql.warehouse.dir",warehouseLocation)

    val spark = SparkSession
                            .builder()
                            .master("local[4]")
                            .appName("bucket_join")
                            .config("spark.sql.shuffle.partitions","1")
                            .config("spark.sql.autobroadcastjointhreshold","-1")
                            .getOrCreate()

    spark.conf.set("spark.sql.autobroadcastjointhreshold","-1")


    println(s"here spark warehouse dir  :: ${spark.conf.get("spark.sql.warehouse.dir")}")

    import spark.sql
    sql("create database FINANCE")
    sql("show databases").show()
    sql("use default").show()
    sql("show tables ").show()

    import spark.implicits._

    val data = Seq(
      (1,"EMP1","2022-07-05"),
      (2,"EMP2","2022-07-06"),
    ).toDF("id","name","dob")
    data.show(false)

    data.write.mode(SaveMode.Append).format("parquet").saveAsTable("FINANCE.emp")

    /*   val table1 = spark.read.table("users_purchase.ds1")
       val table2 = spark.read.table("users_purchase.ds2")
       table1.show()
       table2.show()*/


//    scala.io.StdIn.readLine()
    spark.stop

  }

}
