package spark_practice

import org.apache.spark.sql.{Encoders, SparkSession,Row}
import org.apache.spark.sql.catalyst.expressions.Encode

object FindFaultyRecordEx {

case class Person(age:Int,sal:Int,insurance:Int,amt:Int)

def main(args : Array[String]) : Unit = {


    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","3").master("local[3]").appName("SCD2").getOrCreate()
/*
12,34,56
23,56,86
45,67,56,87
56,77,66

 */

  import spark.implicits._

  val data = Seq(  (12,34,56,1),
                     (23,56,86,1),
                     (45,67,56,1),
                     (56,77,66,1)
     ).toDF
  data.show
  
  val data2 = Seq( Row(12,34,56,1),
    Row(23,56,86,1),
    Row(45,67,56,1),
    Row(56,77,66,1)
  )
 //data2.show

  import spark.implicits._
  val data3 = Seq(  Person(12,34,56,1),
    Person(23,56,86,1),
    Person(45,67,56,1),
    Person(56,77,66,1)
  ).toDS()
  data3.show

   import spark.implicits._

    val rdd = spark.sparkContext.parallelize(data2,2)




     //toDF("col1","col2","col3","col4")

   print(rdd.collect )




    spark.stop





  }

}
