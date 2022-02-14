package spark_practice

import java.util
import java.util.Arrays

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.Row

import scala.xml.MetaData

object array_function {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir","C:\\Spark_3_Software")
      //.enableHiveSupport()
      .getOrCreate()
                       spark.sparkContext.setLogLevel("ERROR")

    println(spark.sparkContext.parallelize(Seq(1, 2), 2).glom().collect())




    val d = Seq("1","2")
    import spark.implicits._
    import org.apache.spark.sql.Encoders._
    //StructType(Array(StructField("id", false, MetaData.Empty))))
     val dd = d.toDF("list")
    dd.show()

    val d1 = List( Row(1,"Anek","1991-07-05") , Row(2,"Dost","1991-10-10") )
    val schema = StructType(
      List(
          StructField("id", DataTypes.IntegerType,false, Metadata.empty),
          StructField("name", DataTypes.StringType,false, Metadata.empty),
          StructField("join_date", DataTypes.StringType,false, Metadata.empty)

      )

    )
    val rdd = spark.sparkContext.parallelize( d1,2 )
    //spark.createDataFrame(d1,schema)
     val df1 =   spark.createDataFrame(rdd,schema)

    import org.apache.spark.sql.functions._
    println("Spark Partition Count  :  ")
    df1.rdd.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show

    val base = spark.sparkContext.parallelize(1 to 100, 4)
    println("Spark Partition data  :  ")
    base.mapPartitionsWithIndex((index, iterator) => {

      iterator.map { x => (index, x) }

    }).foreach { x => println(x) }

    //df.sa

    val mapData = List("ANEK", "VICKY", "NEHA")
    val df = spark.read.option("multiline","true").json("data/sample.json")
    println(System.lineSeparator)
    df.show()



    val values = array(mapData.map(col): _*)
    println(values)

   /* val df_with =
      df
        .withColumn("Emp_Name",$"emp1.EmpName")
        .withColumn("Emp_Age",$"emp1.EmpAge")
        .withColumn("Emp_Gender",$"emp1.EmpGender")
        .withColumn("Emp_Dept",$"emp1.EmpDept")
        .withColumn("EmpDesg",$"emp1.EmpDesg")




    df_with.printSchema
    df_with.show*/

    spark.stop()

  }
}
