package spark_practice.interview_questions

import org.apache.spark.sql.SparkSession

object drop_na {
  def main(args: Array[String]): Unit = {

   System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
   
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("closure_accumulators")
      .config("spark.sql.shuffle.partitions","4")
      //.enableHiveSupport()
      .getOrCreate()

    val ds = spark
      .read
      .options(Map("inferSchema" -> "true","header" -> "true"))
      .csv("./data/customer_account/customer_with_null.csv")

   println(s" count with all :: ${ds.count}")

   import spark.implicits._
   import org.apache.spark.sql.functions._

   println("print with lit null ")
   val ds2 = ds.na.drop().withColumn("null_col",lit(null))
   ds2.show()

   println("print with na.fill NA")
   ds2.na.fill("Not_Available").show()

   ds2.filter($"null_col".isNull).show()


   println(s" count with null :: ${ds2.count}")

   // scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/


    spark.stop()
  }

}
