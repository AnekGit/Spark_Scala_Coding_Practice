package spark_interview_questions

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType}

object print_col_null {
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

    val ds = Seq(
      (1,1,1),(2,2,2),(3,3,3),(4,4,null),(5,5,5),(6,6,6),(7,7,7),(8,8,null),(9,9,9)
    )
      //.to("c1","c2","c3")
    println(ds)



    val ds1 = Seq(10001234,654321).toDF("ref_csf")
    val ds2 = Seq(10001234,654321).toDF("ref_out")
    ds1.printSchema
    ds2.printSchema

    ds1.join(ds2,'ref_csf.equalTo('ref_out),"inner").show()

   // ds1.join(ds2,trim('ref_csf).equalTo( trim('ref_out) ),"inner").show()
   // ds1.join(ds2,'ref_csf.cast(DataTypes.IntegerType).equalTo('ref_out.cast(DataTypes.IntegerType)),"inner").show()

      //ds.toDF("c1,"c2","c3").show


    //ds.show()

              /*
              *
              *
              * For below df
C1 C2 C3
1    1     1
2   2     2
3   3     3
4   4    null
5   5     5
6  6      6
7   7      7
8   8   null
9   9     9

Print null and next to null values in df.
desired output is below
Output:
C1 C2 C3
4    4    null
5    5     5
8    8    null
9    9     9
              * */









    spark.stop

  }

}
