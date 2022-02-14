package spark_practice.rows_to_cols

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
/**
 * created by ANEK on Monday 10/11/2021 at 11:18 AM 
 */

object row_to_cols {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software\\")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir","C:\\Spark_3_Software\\")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import  spark.implicits._
    val s = "ANEKANE"
    // List((a,1),(n,1)....(k,1))
    println(s.split("")
      .map(e => (e ,1) )
      .groupBy(x => x)
    )
    println(
      s.split("")
        .map(e => (e ,1) )
        .groupBy(x => x)
        .mapValues(_.length)
        .map(e => (e._1._1,e._2)).toList
    )



    //s.flatMap( e =>  e.split(""))






    val values = Seq(
                          ("66", "PL", "2016-11-01"),
                          ("66", "PL", "2016-12-01"),
                          ("67", "JL", "2016-12-01"),
                          ("67", "JL", "2016-11-01"),
                          ("67", "PL", "2016-10-01"),
                          ("67", "PO", "2016-09-01"),
                          ("67", "JL", "2016-08-01"),
                          ("68", "PL", "2016-12-01"),
                          ("68", "JO", "2016-11-01")
                     )
      .map(row => (row._1, row._2, row._3) )

    import spark.implicits._
    val df = values.toDF("KEY","CODE","DATE")
    df.show(false)

    import org.apache.spark.sql.functions._
    def sortAndStringUdf =
      udf(
      (codeDate: Seq[Row]) => codeDate.sortBy(row => row.getAs[Long]("DATE"))
                                     .map(row => row.getAs[String]("CODE")).mkString("-")
      )

    val inter = df.withColumn("codeDate",
      struct(col("CODE"),col("DATE").cast("timestamp").cast("long").as("DATE")))

    inter.printSchema()
    inter.show()

      val inter2 = inter.groupBy("KEY")
                       .agg(
                             sortAndStringUdf( collect_list("codeDate") ).as("CODE")
                       )
    val inter3 = inter
                     .withColumn("codeDate", struct(col("DATE").cast("timestamp").cast("long").as("DATE"), col("CODE")))
                     .groupBy("KEY")
                     .agg(
                       concat_ws("-",
                                           sort_array( collect_list($"DATE"),true )
                                                                                   )
                     )
                     //.agg(concat_ws("-", expr("sort_array(collect_list(codeDate)).CODE")).alias("CODE"))
                     .show(false)

    inter2.show(false)
    spark.stop()
  }
}
