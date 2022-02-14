package spark_interview_questions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object click_stream_analysis {
  def main(args: Array[String]): Unit = {
    val conf :SparkConf = new SparkConf().set("spark.sql.shuffle.partitions","3")
      //.set("spark.sql.warehouse.dir","")
      .set("hadoop.home.dir","C:\\winutils")
    val spark = SparkSession.builder()
                            .appName("spark_interview_questions")
                            .master("local[4]")
                            .config(conf)
                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    spark.sql(
      """
        |with ds(n) as
        |   (
        |select 1
        |union all
        |select n+1 from ds
        |where n < = 6
        |    )
        |    select * from ds
        |
        |""".stripMargin).show()



    val ds = Seq(
      ("2021-01-31 00:10:00","u1"),
      ("2021-01-31 01:10:00","u1"),
      ("2021-01-31 01:30:00","u1"),
      ("2021-01-31 02:50:00","u1"),
      ("2021-01-31 03:10:00","u2"),
      ("2021-01-31 04:10:00","u2")
    ).toDF("timestamp","user")
    //ds.show(false)

    val windowSpec = Window.partitionBy(col("user")).orderBy(col("timestamp"))
                           .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

    val ds2 = ds
        .withColumn("lag_value",lag( col("timestamp") ,1).over(windowSpec) )
      //.withColumn("ss",min)
        .withColumn("tdiff",

           lit(
              unix_timestamp( col("timestamp" ) ) - unix_timestamp( col("lag_value") )
            )
            .divide( lit("60").cast(IntegerType) )

        )
        .withColumn("flag",when(col("tdiff").gt(lit("30")),"1").otherwise("0"))
        .withColumn("new_session",
                   concat_ws("_session_",col("user"),sum(col("flag")).over(windowSpec).cast(IntegerType))
        
        )


    ds.createOrReplaceTempView("ds")
    /*  spark.sql(
      """
        |  with t1 as (
        |              select a.*, lag(timestamp,1) over(partition by user order by timestamp) as lag_value from ds a
        |              )
        |  with t2 as (
        |              select t1.*,unix_timestamp(timestamp) - unix_timestamp(lag_value) as tdiff from t1
        |             )
        |  select * from t2
        |
        |""".stripMargin).show()*/


    ds2.show


    spark.stop

  }

}


