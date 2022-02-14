package spark_practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object flatMapEx {

  def main(args : Array[String]) :Unit = {

    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions","2")
    val spark = SparkSession.builder().master("local[3]").appName("flatMapEx")
               .config(conf).getOrCreate()
    
     val data = List("1,a,200INDIA2,b,300INDIA")

    /* Note the   ,   in the flatMap and ,map output */

    val rows = data
      .flatMap(_.split("INDIA"))     // List(1,a,200, 2,b,300)
      .map(_.split(","))  //List(Array(1, a, 200), Array(2, b, 300))
      .map( x => Row( x(0).toInt ,x(1),x(2).toInt  )  )

    val schema = StructType(
      List(
        StructField("ID",IntegerType,false),
        StructField("Name",StringType,false),
        StructField("Sal",IntegerType,false)
      )
    )

    val rdd  = spark.sparkContext.parallelize(rows)
     val df = spark.createDataFrame(rdd,schema)


    df.show

    spark.stop


  }

}
