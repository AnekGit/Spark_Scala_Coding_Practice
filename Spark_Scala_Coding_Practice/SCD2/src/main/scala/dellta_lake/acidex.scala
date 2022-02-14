package dellta_lake

import org.apache.spark.sql.{SaveMode, SparkSession}

object acidex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("Acid").getOrCreate()

         import spark.implicits._

    scala.util.Try(
    r = spark.range(100).repartition(1).map { i =>
      if (i > 50) {
        Thread.sleep(1000)
        throw new RuntimeException("failed")
      }
      i
    }.write.mode(SaveMode.Append).csv("test1.csv") )

    




    spark.stop
  }
}
