package spark_practice.interview_questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object read_multiple_files {
  def main(args: Array[String]): Unit = {

   System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")

    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("read_multiple_files")
      .config("spark.sql.shuffle.partitions","3")
      //.enableHiveSupport()
      .getOrCreate()

    val dsAll = List("./data/customer_account/customer.csv",
    "./data/customer_account/customer1.csv",
    "./data/customer_account/customer2.csv"
    ).map( e => {
      spark
        .read
        .options(Map("inferSchema" -> "true","header" -> "true"))
        .csv(e)
      }
    ).reduce(_ unionByName _ )

    println(s"dsAll count :: ${dsAll.count}")
    dsAll.show()

    import spark.implicits._
    val ds = spark
      .read
      .options(Map("inferSchema" -> "true","header" -> "true"))
      .csv("./data/customer_account/customer.csv")
    ds.show()
    spark.stop
  }
  case class Account(account_no : String ,name :String ,salary : String )
}
