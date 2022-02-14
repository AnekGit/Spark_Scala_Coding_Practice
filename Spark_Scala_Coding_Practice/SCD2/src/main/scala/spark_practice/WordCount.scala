package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("WordCount").getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("This a sample program . And this sample program is a program ."),
      ("This a sample program . And this sample program is a program .")
    )

    val lines = spark.sparkContext.parallelize(data)


    lines
      .flatMap(e => e.split(" "))
      .map(x => (x,1))
      .reduceByKey(_+_)
      .take(10)
    
    //lines.collect

    // lines.map(e => e.split(" ")).map(x => (x,1)).collect()

    //scala.io.StdIn.readLine()

    spark.stop




  }

}
