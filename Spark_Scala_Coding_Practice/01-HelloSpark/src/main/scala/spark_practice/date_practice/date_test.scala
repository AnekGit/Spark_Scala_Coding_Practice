package spark_practice.date_practice
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * created by ANEK on Wednesday 10/13/2021 at 6:17 PM 
 */

object date_test {
  implicit def strToDate(str :String ): java.sql.Date = java.sql.Date.valueOf(str)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software\\")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir","C:\\Spark_3_Software\\")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val data = Seq("2021-07-01","2021-07-31","2021-07-10","2021-07-20").toDF("c1")

    data.withColumn("dates_only",date_format(col("c1"),"yyyy")).show(false)

    Seq("01-10-2021","02-10-2022").toDF("c1")
      .withColumn("dates_only",to_date(col("c1"),"mm-dd-yyyy")).show()


    Seq("2021-10-01","2022-01-10").toDF("c1")
      .withColumn("dates_only",to_date(col("c1"))).show()

    Seq("2021-10-01 01:22:30 AM").toDF("c1")
      .withColumn("dates_only",
        to_timestamp(
          col("c1"),"yyyy-mm-dd hh:mm:ss a"
        )
      ).show()

    Seq("2021-10-01 11:22 AM").toDF("c1")
      .withColumn("dates_only",
        to_date(
          from_unixtime(
                       unix_timestamp(col("c1"),"yyyy-mm-dd HH:MM AM")
          )

        )
      ).show()

    data.show
    data.printSchema

    val monthStart = "2021-07-01"
    val monthEnd = "2021-07-31"

    val prevMonth = LocalDate.parse(monthStart).minusMonths(1)
    val prevMonthStart = prevMonth.withDayOfMonth(1)
    val prevMonthEnd = prevMonth.withDayOfMonth(prevMonth.getMonth.maxLength())

    //val prevMonth = LocalDate.parse(monthStart).withDayOfMonth( prevMonth)

    println(s"previous prevMonthStart  :: ${prevMonthStart.toString}")
    println(s"previous prevMonthEnd  :: ${prevMonthEnd.toString}")




    data.withColumn("row",monotonically_increasing_id())
    .withColumn("c1_add",date_add($"c1",10))
    .withColumn("c1_sub",date_sub($"c1",10))
    .withColumn("c1_diff", col("c1").gt(monthStart))
    //.filter(col("row").g).show()

    
    data.withColumn("c1_curr", when(
                                          date_add($"c1",1).between(monthStart ,monthEnd )
                                           ,lit("current")
                                        ) .when(
                                              date_add($"c1",1).between(monthStart ,monthEnd )
                                              ,lit("current")

                                               )

    ).show()
    spark.stop

  }
}
