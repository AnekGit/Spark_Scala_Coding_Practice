package tesco
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

case class Salary(depName: String, empNo: Long, salary: Long)
object top3sell {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","3").master("local[3]").appName("SCD2").getOrCreate()
    import spark.implicits._

    val empsalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)).toDS

    val byDepName = Window.partitionBy('depName).orderBy("salary")
    empsalary
      .withColumn("avg", dense_rank().over(byDepName).as("rnk") )
      .filter('rnk < 4).show

         //  TOP SELLING PRODUCTS REGION WISE

    var dataset = Seq(
      ("Normal",     "cell phone", 6000),
      ("Normal",     "tablet",     1500),
      ("Mini",       "tablet",     5500),
      ("Mini",      "cell phone",  1000),
      ("Thin",      "cell phone",  5000),
      ("Thin",      "cell phone",  6000),
      ("Big",        "tablet",     2500),
      ("Bendable",   "cell phone", 3000),
      ("Bendable",   "cell phone", 3000),
      ("Pro",        "tablet",     4500),
      ("Pro",       "tablet",     6500))
      .toDF("product", "category", "revenue")

       /*
       *
       * Store_id	Product_id	Product_name	  Amount	  Units	   Region
          1				      P1			  Mobile_1	    1000		  1	    Central
          1				      P2			  Mobile_2	    1000		  10	  Central
          1				      P2			  Mobile_2	     500		  10	  Central
          1				      P1			  Mobile_1	    20000		  20	  South
          1				      P1			  Mobile_1	    100000		100	  Central
          1				      P1			  Mobile_2	    2000000		2000	South

       *
       *
       * */

    dataset = dataset.groupBy("category","product").agg(sum("revenue").as("total_cnt"))
    val windowsSpec = Window.partitionBy("category").orderBy($"total_cnt".desc)

    dataset.withColumn("top_selling",dense_rank().over(windowsSpec) ).show










    spark.stop

  }

}
