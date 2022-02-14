package spark_practice.avoid_joins
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM
 */
object bucket_joins {

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
      .config("spark.sql.autobroadcastjointhreshold","-1")
     // .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    sql("show databases").show()
    sql("show tables").show()
    /*spark.catalog.listTables().show(false)
    spark.table("default.managed_ds1").show()
*/
  /*  val path = "\\part-00000-f9534365-02b6-440a-842a-0993fe7850a3_00001.c000.snappy.parquet"
    spark.read.parquet("F:\\Spark_workspace\\SparkProgrammingInScala-master\\01-HelloSpark\\spark-warehouse\\users_purchase" +
      ".db\\ds1\\users_purchase_date=2021-12-30"+path).show(false)*/

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
      ) ,4
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
     ds1.write.option("path","./spark-warehouse/").saveAsTable("mananged_ds1_1")




     val ds2 = ds1.columns.foldLeft(ds1)(
       (ds1,columnName) => ds1.withColumn(columnName.concat("_rt"), col(columnName) ).drop(columnName)
       )

     import spark.sql
     sql("create database if not exists users_purchase")
     sql("use users_purchase")

    println("check partition wise count  before bucket and " +
      "saving ________________________________________________________")

    ds1.rdd.glom().map(_.mkString("->")).foreach(println)
    ds1.groupBy(spark_partition_id()).agg(count("*")).show()
    

    ds2.rdd.glom().map(_.mkString("->")).foreach(println)
    ds2.groupBy(spark_partition_id()).agg(count("*")).show()




    println("______________________________________________________________________________________")

     ds1
       .write
       .partitionBy("users_purchase_date")
       .sortBy("id")
       .bucketBy(20,"id")
       .saveAsTable("users_purchase.ds1")

     ds2//.repartition(20)
       .write
       .partitionBy("users_purchase_date_rt")
       .sortBy("id_rt")
       .bucketBy(10,"id_rt")
       .saveAsTable("users_purchase.ds2")

     sql("show tables").show()

     val table1 = spark.read.table("users_purchase.ds1")
     val table2 = spark.read.table("users_purchase.ds2")

    println("after loading table from disk -------------------------------------")
    table1.rdd.glom().map(_.mkString("->")).foreach(println)
    table2.rdd.glom().map(_.mkString("->")).foreach(println)

     spark.conf.set("spark.sql.autobroadcastjointhreshold","-1")
     val ds = table1.where("users_purchase_date='2021-12-30'")

     println(s"spark plan :: ${ds.queryExecution.sparkPlan}")
     println(s"executed plan :: ${ds.queryExecution.executedPlan}")
     println(s"number of partitions [table1] :: ${ds.rdd.getNumPartitions}")
    ds.selectExpr("'anek' as name2").show(false)


     table1.where("users_purchase_date='2021-12-30'").show(false)
     println(s"number of partitions [table2] :: ${table2.rdd.getNumPartitions}")

     val finalDS = table1.join(table2,col("id") === col("id_rt"),"inner")
     val ds_table2 = table2.where("users_purchase_date_rt='2021-12-30'")

     println(s"spark plan :: ${ds_table2.queryExecution.sparkPlan}")
     println(s"executed plan :: ${ds_table2.queryExecution.executedPlan}")
     println(s"number of partitions [table2] :: ${ds_table2.rdd.getNumPartitions}")

     println(s"join finalDS spark plan :: ${finalDS.queryExecution.sparkPlan}")
     println(s"join finalDS executed plan :: ${finalDS.queryExecution.executedPlan}")
     println(s"join finaDS no of partitions :: ${finalDS.rdd.getNumPartitions}")

     finalDS.foreach(e => ())
     scala.io.StdIn.readLine()
     spark.stop

  }

}
