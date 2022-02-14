package spark_practice

import org.apache.spark.sql.functions._
import java.sql.Date

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}



object spark_joins {
  case class CustomerUpdate(customerId: Int,
                            address: String,
                            effectiveStartDate: Date)

  case class Customer(customerId: Int,
                      address: String,
                      current: String,
                      effectiveStartDate: Date,
                      effectiveEndDate: Date)
  def main(args: Array[String]): Unit = {

    implicit def date(str: String): java.sql.Date = java.sql.Date.valueOf(str)
    implicit def strr(str: Boolean): String = String.valueOf(str)
    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software\\");
    val spark = SparkSession.builder().master("local[4]")
      .appName("array_function")
      .config("spark.sql.shuffle.partitions",2)
      .getOrCreate()

    import spark.implicits._

    var currentDS = Seq(
      Customer(1, "old address for 1", false, null, "2018-02-01"),
      Customer(1, "current address for 1", true, "2018-02-01", null),
      Customer(2, "current address for 2", true, "2018-02-01", null),
      Customer(3, "current address for 3", true, "2018-02-01", null),
      Customer(3, "latest address for 3", true, "2018-02-01", null),
      Customer(3, "amazing address for 3", true, "2018-02-01", null),
      Customer(3, "amazing1address for 3", true, "2018-02-01", null),
      Customer(3, "amazing2 address for 3", true, "2018-02-01", null)

    ).toDS()
    //currentDS = currentDS.repartition(3,$"customerId")
    //currentDS.show(false)

    println(s"currentDS number of partitions :: ${currentDS.rdd.getNumPartitions}")

    val windowSpec = Window.partitionBy().orderBy("customerId")
    val windowDS = currentDS.withColumn("window_col",row_number().over(windowSpec))
    windowDS.foreach(e => ())

    println(s"after window :: ${windowDS.rdd.getNumPartitions}")
    println(s"spark plan  :: ${currentDS.queryExecution.sparkPlan}")
    println(s"executed plan  :: ${currentDS.queryExecution.executedPlan}")



    scala.io.StdIn.readLine()
     //currentDS.withColumn("row",rand() % currentDS.rdd.getNumPartitions).show()
  /*   currentDS
      .write.mode(SaveMode.Overwrite)
      //.partitionBy("customerId")
      .bucketBy(3,"customerId")
      .saveAsTable("ust")
*/



    






    /*println(currentDS.rdd.collect().toList.toString())

    val newDS = Seq(
      CustomerUpdate(1, "new address for 1", "2018-03-03"),
      CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
      CustomerUpdate(4, "new address for 4", "2018-04-04")
    ).toDS()
   // newDS.createOrReplaceTempView("new_customers")
    newDS.show(false)

   val exp = Seq("right.customerId","right.address","right.current","right.effectiveStartDate","right.effectiveEndDate")
    val expLeft = Seq("left.customerId","left.address","left.effectiveStartDate")


    val letAntiDS = newDS.as("left")
        .join(
          currentDS.filter($"effectiveEndDate".isNull).as("right"),
                                   $"left.customerId".equalTo($"right.customerId"),
                                                                                         "leftanti"
          )
    println("latest DS insert ===>")
    val insertDS = letAntiDS.select("left.*")
                                                   .withColumn("current",lit("true"))
                                                   .withColumn("effectiveEndDate",functions.lit(null))

    
    val insertDSFinal = insertDS.as("right").select(exp.map(col(_)):_*)
    insertDSFinal.show(false)

     val retainedDS = newDS.as("left")
         .join(
           currentDS.as("right") , $"left.customerId".equalTo($"right.customerId")
             .and($"left.address".equalTo($"right.address"))
           ,"inner"
              )
    println("inner DS  -> Retained records ===>")
    val retainedDSFinal = retainedDS.select(exp.map(col(_)):_*)
    retainedDS.show(false)

    
    
    val deletedDS = currentDS.as("left")
        .join(newDS.as("right"),$"left.customerId".equalTo($"right.customerId"),"leftanti"

        ).withColumn("current",lit("false"))
        .withColumn("effectiveEndDate",current_date())
    println("right anti DS  -> Deleted records ===>")
    deletedDS.show(false)



    val updateDS = newDS.as("left")
      .join(
        currentDS.as("right") , $"left.customerId".equalTo($"right.customerId")
          .and($"left.address".notEqual($"right.address"))
        ,"inner"
      )
    println("inner DS  -> Update records ===>")


    val updateDSFinal = updateDS
      .select("right.*")
      .withColumn("right.current",lit("false"))
      .withColumn("right.effectiveEndDate", when($"right.current".equalTo("true"),current_date() )
      ).drop("current","effectiveEndDate")
      .withColumnRenamed("right.current","current")
      .withColumnRenamed("right.effectiveEndDate","effectiveEndDate")
        .select("customerId","address","current","effectiveStartDate","effectiveEndDate")
    updateDSFinal.show(false)

    //union then all

     updateDSFinal
       .union(deletedDS)
       .union(retainedDSFinal)
       .union(insertDSFinal)
       .show(false)

    */
    spark.stop()

  }
}
