package spark_practice.avoid_joins

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM
 */
object bucket_joins2 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val list = List(1,2,3)
    println(list)
    //-Dlog4j.configuration=file:log4j.properties -Dlogfile.name=bucket-spark -Dspark.yarn.app.container.log.dir=app-log


  val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("bucket_join")
      .config("spark.sql.shuffle.partitions","1")
      .config("spark.sql.autobroadcastjointhreshold","-1").getOrCreate()
    spark.conf.set("spark.sql.autobroadcastjointhreshold","-1")

      val path = "\\part-00000-f9534365-02b6-440a-842a-0993fe7850a3_00001.c000.snappy.parquet"
      spark.read.parquet("F:\\Spark_workspace\\SparkProgrammingInScala-master\\01-HelloSpark\\spark-warehouse" +
        "\\users_purchase" +
        ".db\\ds1\\users_purchase_date=2021-12-30").show(false)

    println(s"here spark warehouse dir  :: ${spark.conf.get("spark.sql.warehouse.dir")}")

    import spark.sql
    sql("show databases").show()
    sql("use default").show()
    sql("show tables ").show()

 /*   val table1 = spark.read.table("users_purchase.ds1")
    val table2 = spark.read.table("users_purchase.ds2")
    table1.show()
    table2.show()*/
    













    scala.io.StdIn.readLine()




    spark.stop

  }

}
