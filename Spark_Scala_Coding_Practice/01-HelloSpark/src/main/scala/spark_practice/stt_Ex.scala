package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object stt_Ex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("caching").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val ds=  Seq(
      (111,"HAN",201),
      (111,"HCM",201),
      (112,"HAN",202),
      (112,"HCM",202),
      (113,"HAN",205),
      (113,"HCM",205)

    ).toDF("id","branch_name","branch_code")

    val windowSpec = Window.partitionBy($"id")
   

    val dsgrp = ds.groupBy($"id".as("id_grp"))
                .agg(count("*").as("cnt"))
                .withColumn("stt",row_number().over(Window.orderBy($"id_grp")))
    dsgrp.show

    ds.join( dsgrp,$"id".equalTo($"id_grp"),"inner" )
      .drop("cnt","id_grp").show

    spark.stop

  }

}
