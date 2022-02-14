package spark_practice.dataset.scala

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel

object customer_sales {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software\\")

    val spark = SparkSession.builder()
      .master("local[3]")
      .config("spark.sql.shuffle.partitions","3").getOrCreate()
    import spark.implicits._

    val ds = spark
      .read
      .option("inferSchema",true)
      .option("header",true)
      .json("./data/sales/customer_sales")        // 1 job

   var custSalesDs = ds.as[Customer_Sales2].repartition(3,$"id").cache()

    // 2 job    2 stages -> wide transformation :: repartition and  narrow transformation :: after filter

    custSalesDs.filter(_.id > 20).show()

    // 3rd  job
     val grpDs = custSalesDs
        .filter(_.email.contains("@hotmail"))
        .groupBy($"country").agg(count("*"))
    
    grpDs.collectAsList().forEach(println)

    //grpDs.explain("cost")
    

    //custSalesDs.filter(_.email.contains("@hotmail")).show()           // 3 job



   // custSalesDs.toDF(custSalesDs.columns:_*).except(ds).show()

     scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/
    spark.stop()

  }

}
case class Customer_Sales1(id:Int,name:String,age:String,sales_date:String,country:String)
case class Customer_Sales2(
                           id:BigInt,
                           first :String,
                           last  :String,
                           email :String,
                           created_at :java.sql.Timestamp,
                           company :String,
                           country :String
                          )