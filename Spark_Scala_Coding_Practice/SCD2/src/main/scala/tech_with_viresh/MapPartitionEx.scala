package tech_with_viresh
package danske_it

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
object MapPartitionEx {

  def main(args : Array[String]) : Unit ={

    val spark = SparkSession.builder().config("spark.sql.shuffle.partitions","2")
      .master("local[3]").appName("LatestRecord").getOrCreate()


    /*val data1 = Seq(1,0,1,0,1).toDF("id1")
    val data2=  Seq(1,1,0,0,1).toDF("id2")
    */
    import spark.implicits._
    val list = (1 to 10).toList
    val df = list.toDF("col_1")

    val df1 = df.repartition(3).rdd.mapPartitions( (itr) => Iterator(itr.length) )
    //val df1 = df.repartition(3).rdd.mapPartitionsWithIndex( (index,itr) => Iterator(index,itr.length) )
    df1.collect

    spark.stop

  }
}
