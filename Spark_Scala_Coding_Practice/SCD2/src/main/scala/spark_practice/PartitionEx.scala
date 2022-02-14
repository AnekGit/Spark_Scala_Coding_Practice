package spark_practice

import org.apache.spark.sql.SparkSession

object PartitionEx {

  def main(args : Array[String]): Unit ={

    val spark = SparkSession.builder().master("local[3]").appName("PartitionEx").getOrCreate()

    val x = 1 to 10

    import spark.implicits._
    val data = spark.sparkContext.parallelize(x)

    val df = data.toDF("data")

    println(data.partitions.length)

    df.repartition(2).show
    
    spark.stop

  }


}
