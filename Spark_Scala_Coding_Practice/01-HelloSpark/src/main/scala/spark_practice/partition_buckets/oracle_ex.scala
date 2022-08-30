package spark_practice.partition_buckets

import org.apache.spark.sql.SparkSession

/**
 * created by ANEK on Wednesday 2/23/2022 at 4:47 PM 
 */
    import org.apache.spark.sql.functions._
object oracle_ex {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software\\")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir","C:\\Spark_3_Software\\")
      //.enableHiveSupport()
      .getOrCreate()

    var ds1 = spark.emptyDataFrame

    ds1 = ds1.withColumn("col1",lit("I am anek"))
             .withColumn("new_col1",explode(split(col("col1")," ")))


    ds1.show(false)

    //  anek => singh
    // bill => gates

  /*  1 =>
    2 => linked list
    3 =>
    4
    */



  }

}
