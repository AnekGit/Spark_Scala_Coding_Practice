package spark_practice
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM 
 */
object fold_spark_ex {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("fold_spark").getOrCreate()

    import spark.implicits._
    val data = spark.sparkContext.parallelize(
      Seq(
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-29"),30),
        Row(2,"Dev",java.sql.Date.valueOf("2021-05-14"),14),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-07-05"),30),

      )
    )
    val schema = StructType(
      Seq(
        StructField("id",IntegerType,false),StructField("name",StringType,false),
        StructField("dob",DateType,false),StructField("age",IntegerType,false)
      )
    )
    val ds1 = spark.createDataFrame(data,schema)
    ds1.show()

    val ds2 = ds1.withColumn("review",lit("8.5")).withColumn("result",lit("PASS"))
    ds2.show

    val colDiff12Seq = ds2.columns.diff(ds1.columns).toList
    val colDiff21Seq = ds1.columns.diff(ds2.columns).toList

    println(s"colDiff12Seq ::  ${colDiff12Seq}")
    colDiff12Seq.foreach(println)
    colDiff21Seq.foreach(println)

    val ds_1_final = colDiff12Seq.foldLeft(ds1){
      (ds1,colAdd)  => ds1.withColumn(colAdd,lit(null))
    }
    println("ds1 final : added with new cols")
    ds_1_final.show(false)




    spark.stop














  }


}
