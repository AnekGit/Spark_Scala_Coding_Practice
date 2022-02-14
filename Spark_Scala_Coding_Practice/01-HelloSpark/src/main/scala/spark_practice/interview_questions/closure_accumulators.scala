package spark_practice.interview_questions

import org.apache.spark.sql.SparkSession

object closure_accumulators {
  def main(args: Array[String]): Unit = {

   System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
   
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("closure_accumulators")
      .config("spark.sql.shuffle.partitions","4")
      //.enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    var counter = 0L
    val data = List(1L,2L,3L,4L,5L)
    val ds = spark.sparkContext.parallelize(data,5).foreach(e => counter+=e)
    //ds.show()
    println(s"counter :: ${counter}")

    val accumulator = spark.sparkContext.longAccumulator("my_acc")
    val ds2 = spark.sparkContext.parallelize(data,5).foreach(e => accumulator.add(e))

    println(s"accumulator :: ${accumulator.value}")


    scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/


    spark.stop()
  }

}
