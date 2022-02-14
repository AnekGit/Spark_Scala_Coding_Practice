package danske_it

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
object InnerJoin {

  def main(args : Array[String]) : Unit ={

    val spark = SparkSession.builder().config("spark.sql.shuffle.partitions","2")
      .master("local[3]").appName("LatestRecord").getOrCreate()

    import spark.implicits._
    /*val data1 = Seq(1,0,1,0,1).toDF("id1")
    val data2=  Seq(1,1,0,0,1).toDF("id2")
    */
    val data1 = Seq("A",null,"C") .toDF("id1")
    val data2= Seq("A","C","A",null) .toDF("id2")

    data1.join(data2,col("id1").equalTo(col("id2"))).show
    println(data1.join(data2,col("id1").equalTo(col("id2"))).count)

      /*   union example */
    val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
    val df2 = Seq((1, 2, 4)).toDF("col1", "col2", "col0")
     df1.union(df2).dropDuplicates(Seq("col0","col1")).show

    df1.union(df2).distinct().show



    spark.stop

  }
}
