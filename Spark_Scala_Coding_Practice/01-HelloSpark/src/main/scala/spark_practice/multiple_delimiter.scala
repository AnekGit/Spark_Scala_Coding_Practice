package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object multiple_delimiter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("Multiple_Delimiter").getOrCreate()

    import  spark.implicits._
    spark.read
      .option("inferSchema","True")
      .option("header","true")
      .option("delimiter","~|")
      .csv("data/multi_delimiter.csv")

      // different method
    val df = spark.read.text("data/multi_delimiter.csv")
    df.show()
    val first  = df.first()(0)
    val schema = first.toString.split("~\\|")

    println(first.toString)

    println("for loop ******************")
   for(e <- schema)   println(e)


    val df_without_schema = df.filter( ! col("value").startsWith("Name") )

                   df_without_schema
                     .map(_.toString())
                     .flatMap(_.split("~\\|") )
                     .toDF("Names").show


     spark.stop()
  }

}
