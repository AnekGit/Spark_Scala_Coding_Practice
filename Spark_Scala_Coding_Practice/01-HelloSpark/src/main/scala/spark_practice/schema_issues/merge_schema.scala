package spark_practice.schema_issues

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * created by ANEK on Monday 3/7/2022 at 10:51 AM 
 */

object merge_schema {
  def main(args: Array[String]): Unit = {


    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software")
    System.setProperty("spark.sql.warehouse.dir",warehouseLocation)
    
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local[3]").appName("Merge_Schema").getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1,"EMP1","2022-07-05"),
      (2,"EMP2","2022-07-06"),
    ).toDF("id","name","dob")
    data.show(false)

    println(s"here spark warehouse dir  :: ${spark.conf.get("spark.sql.warehouse.dir")}")


    import spark.sql
    sql("create database if not exists CLIENT")
    sql("show databases").show()
    sql("use default").show()
    sql("show tables ").show()
     println("*****************************")
    data.createOrReplaceTempView("emp")
    sql(
      """ with cte (
        |                select 100 as id 
        |                union all
        |                select id from emp
        |               ) select * from cte
        |
        |""".stripMargin).show


    data.write.saveAsTable("client.emp1")



    spark.stop

  }

}
