package tech_with_viresh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object walmartTest {

  def main(args: Array[String]): Unit = {

    //  Employee   id  name salary sex


    val spark = SparkSession.builder().master("local[3]").appName("walmart_test").getOrCreate()

    val df = spark.read.option("header","true").option("inferSchema","true").csv("data/employee.csv")

    val ds = df
      .withColumn("sex",
        when(col("sex").equalTo( lit("F")),lit("M") ).otherwise(lit("F"))

    )
    val windowSpec = Window.orderBy("")
   // ds.withColumn("id",)
     ds.show
    
    spark.stop


  }




}
