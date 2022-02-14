package spark_practice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM 
 */
object left_semi_join_ex {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("fold_spark")
      .config("spark.sql.shuffle.partitions","2").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.parallelize(
      Seq(
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-29"),30),
        Row(2,"Dev",java.sql.Date.valueOf("2021-05-14"),14),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-07-05"),30),
        Row(4,"Nanhe",java.sql.Date.valueOf("2021-02-08"),28)
      ) ,4
    )
    val schema = StructType(
      Seq(
        StructField("id",IntegerType,false),StructField("name",StringType,false),
        StructField("dob",DateType,false),StructField("age",IntegerType,false)
      )
    )
    val ds1 = spark.createDataFrame(data,schema)
    ds1.withColumn("number_format1",round(lit("345765.6543789"),5))
      .withColumn("number_format2",format_number(
        lit("345765.6543789").cast(DataTypes.createDecimalType(38,10)),5)
      )
      .show

    val ds2 = ds1.columns.foldLeft(ds1)(
      (ds1 , columnName) => {
        ds1.withColumn(columnName.concat("rt"), col(columnName) )
      }
    )
    //ds2.show()
    val finalDS = ds1.join(ds2.where($"age" === 30),Seq("id"),"inner")

    finalDS.foreach(e => ())
    scala.io.StdIn.readLine()
    spark.stop

  }

}
