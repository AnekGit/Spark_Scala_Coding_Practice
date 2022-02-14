package tech_with_viresh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object click_stream_analysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","3").master("local[3]").appName("SCD2").getOrCreate()
    import spark.implicits._

    var data = Seq(
      ("2020-07-06 01:20:21","u1"),
      ("2020-07-07 01:25:21","u1"),
      ("2020-07-06 01:35:21","u1") ,
      ("2020-07-06 01:50:21","u1"),
      ("2020-07-06 01:55:21","u1"),
      ("2020-07-06 01:55:21","u2"),
      ("2020-07-06 01:25:21","u2"),

    ).toDF("click_stream","user")

    val windowSpec = Window.partitionBy("user").orderBy("click_stream")

    data = data.withColumn("lag",lag("click_stream",1).over(windowSpec) )

                                                      

    data =  data.withColumn("time_diff", ( ( unix_timestamp($"click_stream") -unix_timestamp($"lag") ) /60  ) )

    data = data.withColumn("session_info",when($"time_diff".gt(10),lit("1")  ).otherwise(lit("0"))
    )

     data.show
    spark.stop






  }

}
