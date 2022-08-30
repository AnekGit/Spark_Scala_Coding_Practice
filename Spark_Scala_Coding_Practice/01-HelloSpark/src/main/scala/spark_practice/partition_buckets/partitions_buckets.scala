package spark_practice.partition_buckets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.reflect.io.File

/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM
 */
object partitions_buckets {

  val spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("partitions_buckets")
    .config("spark.sql.shuffle.partitions","3")
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    //.enableHiveSupport()
    // C:\temp\hadoop\bin\winutils.exe chmod 777 \tmp\hive  C:\Spark_3_Software\bin\winutils.exe chmod 777 \tmp\hive
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    //-Dlog4j.configuration=file:log4j.properties -Dlogfile.name=bucket-spark -Dspark.yarn.app.container.log.dir=app-log
    import spark.implicits._

    /* import spark.sql

     sql("show databases").show()
     sql("show tables").show()
     spark.catalog.listTables().show(false)
     spark.table("default.managed_ds1").show()
 */
   val data = spark.sparkContext.parallelize(
      Seq(
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-30"),30),
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-30"),30),
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-30"),30),
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-30"),30),
        Row(2,"Dev",java.sql.Date.valueOf("2021-11-30"),14),
        Row(2,"Dev",java.sql.Date.valueOf("2021-11-30"),14),
        Row(2,"Dev",java.sql.Date.valueOf("2021-11-30"),14),
        Row(2,"Dev",java.sql.Date.valueOf("2021-11-30"),14),
        Row(2,"Dev",java.sql.Date.valueOf("2021-11-30"),14),
        Row(2,"Dev",java.sql.Date.valueOf("2021-11-30"),14),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-12-30"),30),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-12-30"),30),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-12-30"),30),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-12-30"),30),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-10-30"),28),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-10-30"),28),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-10-30"),28),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-10-30"),28),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-10-30"),28),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-10-30"),28)
      ) ,3
    )
  val schema = StructType(
      Seq(
        StructField("id",IntegerType,false),
        StructField("name",StringType,false),
        StructField("users_purchase_date",DateType,false),
        StructField("age",IntegerType,false)
      )
    )
    val ds1 = spark.createDataFrame(data,schema)
    ds1.show(false)
    //ds1.write.mode(SaveMode.Overwrite).save("./users")


    ds1
       .write
       .partitionBy("users_purchase_date")/*
       .sortBy("id")
       .bucketBy(1,"id")*/
       .saveAsTable("users_purchase_bucketed")

    //partitionWiseData(lkp_ds)

     scala.io.StdIn.readLine()
     spark.stop

  }
  /*
  def partitionWiseData(lkpUpDs : Dataset[Row]) = {
   import spark.implicits._

    lkpUpDs.rdd.glom().map(_.mkString("\n->")).foreach(println)
    println(s"lkpUpDs.rdd.getNumPartitions :: ${lkpUpDs.rdd.getNumPartitions}")
    println("Get number of records for each partitions ==> ")

    //1st way .
    lkpUpDs.groupBy(spark_partition_id).count().show(false)

    // 2nd way .
    lkpUpDs
      .rdd
      .mapPartitionsWithIndex{ (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","data_count")
      .show(false)


  }
*/

}
