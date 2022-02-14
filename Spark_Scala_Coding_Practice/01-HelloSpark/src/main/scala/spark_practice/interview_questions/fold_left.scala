package spark_practice.interview_questions
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}


object fold_left {
  case class Person(name:String,salary:Double)
  def main(args: Array[String]): Unit = {

   System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software\\")
   
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("fold_left_ex")
      .config("spark.sql.shuffle.partitions","4")
      //.enableHiveSupport()
      .getOrCreate()

   // METHOD 1 ::
   println(s"METHOD 1 :: ")
   import spark.implicits._
   val columns1 = Seq("A","B","C")
   val data1 = Seq( Tuple3(1,2,3) ).toDF( columns1:_* )
   data1.show()

   // METHOD 2 ::
   println(s"METHOD 2 :: ")
   import scala.collection.JavaConversions._
   val data2 : Seq[Row] = List( Row(1,2,3),Row(5,6,7) )

   val schema2 :StructType = StructType(
                                           List(
                                            StructField("A",IntegerType,true),
                                            StructField("B",IntegerType,true),
                                            StructField("C",IntegerType,true)
                                           )
                                       )

   val ds : Dataset[Row] = spark.createDataFrame(data2,schema2)
   ds.show()

   // METHOD 3 ::
   println(s"METHOD 3 :: ")
   val rdd3 =  spark.sparkContext.parallelize(data2,3)
   rdd3.glom().foreachPartition(e => println(e.length))

   val ds3 :Dataset[Row] = spark.createDataFrame(rdd3,schema2)

   // METHOD 4 :: create dataset
   println(s"METHOD 4 :: ")
   val personData = Seq(
                         Person("anek",10000.0),
                         Person("neha",50000.0)
                       )
   val ds4 : Dataset[Person] = spark.createDataset(personData)
   ds4.show()
   spark.createDataset(personData).map( p=>(p.name,p.salary)).toDF("name","sal").show()























  // val ds2 = Seq("4","5","3").toDF("D","E","C")

  // val diffCols = ds1.columns.diff(ds2.columns).toSet

/*   ds1.show()
   ds2.show()
   println(s"ds1.columns :: ${ds1.columns}")
   println(s"ds1.columns :: ${ds2.columns}")*/
   //println(s" difference in cols :: ${diffCols}")


   // scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/


    spark.stop()
  }

}
