package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("hadoop.home.dir", "C:\\Spark_3_Software")
      .appName("mapPartitionsWithIndex")
      .master("local[*]")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val ds = spark.read.parquet("./data/emp_partitioned").cache()
     ds.printSchema()
   // println("ds.rdd.getNumPartitions :: "+ds.rdd.getNumPartitions)

    println("===============  partitionId.show  ================")
    ds.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count.show()

    println("\n mapPartitionWithIndex ::  partition and records ")
    ds.rdd.mapPartitionsWithIndex( (index, iterator) => iterator.map(x => (index,x)) )
   .foreach(println)
    /*
 .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
 .toDF("partition_number","number_of_records")
 .show


     println("\n mapPartitionWithIndex ::  partition and count ")
     ds.rdd.mapPartitionsWithIndex( (index, rowsiterator) => Iterator( (index,rowsiterator.size) )  ).foreach(println)


    println("\n mapPartitionWithIndex :: 1")
    ds.rdd.mapPartitionsWithIndex( (index,iterator) => {
      iterator.toList.map(x =>  if (index == 1)  {println(x)}  ).toIterator
    }
    ).collect().foreach(println)
 */
    scala.io.StdIn.readLine()
    //Thread.sleep(10000)

    spark.stop

  }

}
