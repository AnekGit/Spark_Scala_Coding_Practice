package spark_practice

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SchemaEx {

case class Person(age:String,sal:Int,insurance:Int,amt:Int)

def main(args : Array[String]) : Unit = {


    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","3").master("local[3]").appName("SCD2").getOrCreate()

  val data = Seq(
    Row(1, "a"),
    Row(5, "z")
  )

  val schema = StructType(
    List(
      StructField("num", IntegerType, true),
      StructField("letter", StringType, true)
    )
  )
  val read = spark.read.option("header","true").csv("ingest_csv")

  val df = spark.createDataFrame( spark.sparkContext.parallelize(data), schema)

  df.show()

    


    spark.stop





  }

}
