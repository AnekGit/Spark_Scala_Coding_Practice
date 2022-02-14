package spark_practice
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window

// maximum amount debited for each customer on the latest sale date
object cutomer_account_calc {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software\\")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("customer_account")
      .config("hadoop.home.dir","C:\\Spark_3_Software\\")
    //  .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
       import spark.implicits._
    

         val d = Seq("01001","02001").toDF("c1")
           d.withColumn("t",
                   concat(lit("V"),substring($"c1",1,2) )

           ).show





    var ds = spark.read
       .option("header",true)
       .option("inferSchema",true)
       .csv("data/customer_account")

    ds.printSchema()
    ds.show()

    val filtered_df = ds.columns.map( e => col(e.replace(" ","_")) )
    filtered_df.foreach(println)

    val dsNew = for( c <- ds.columns){
        ds =  ds.withColumnRenamed(c,c.replace(" ","_") )
    }
   ds = ds.withColumn("Date",to_date($"Date","m/d/yyyy"))
          .filter(col("Transaction_Type").equalTo("debit"))
   ds.show

    val windowSpec = Window.partitionBy("Customer_No").orderBy($"Date".desc).orderBy($"Amount".desc)

    ds.withColumn("rank",rank().over(windowSpec))
      .where("rank = 1 ").show()


    spark.stop()

  }
}
