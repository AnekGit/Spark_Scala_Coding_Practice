
package spark_interview_questions
import java.sql.Date

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


object scd_2 {
  case class CustomerUpdate(customerId: Int,
                            address: String,
                            effectiveStartDate: Date)

  case class Customer(customerId: Int,
                      address: String,
                      current: Boolean,
                      effectiveStartDate: Date,
                      effectiveEndDate: Date)

  implicit def date(str: String): java.sql.Date = java.sql.Date.valueOf(str)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software");
    val spark = SparkSession.builder().master("local[3]").appName("SCD2_Impl")
      .config("spark.sql.shuffle.partitions",3).getOrCreate()

    import spark.implicits._
    val lookUpDS : Dataset[Customer] =
      Seq(
      Customer(1, "old address for 1", false, null, "2018-02-01"),
      Customer(1, "current address for 1", true, "2018-02-01", null),
      Customer(2, "current address for 2", true, "2018-02-01", null),
      Customer(3, "current address for 3", true, "2018-04-04", null)
    ).toDS()
    //lookUpDS.printSchema()
    lookUpDS.show(false)

    val newDS : Dataset[CustomerUpdate] =
      Seq(
      CustomerUpdate(1, "new address for 1", "2018-03-03"),
      CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
      CustomerUpdate(4, "new address for 4", "2018-04-04")
    ).toDS()
    newDS.show(false)

    val join_key : String = "customerId"
    val business_key : Array[String] = Array("address")

    println("=========== inserted records =================")
    val insert_ds = newDS.join(lookUpDS,Seq(join_key),"left_anti")
    val insert_ds_final =
      insert_ds
          .withColumn("current",lit(true))
          .withColumn("effectiveStartDate",current_date())
          .withColumn("effectiveEndDate",lit(null))
          .withColumn("flag",lit("Inserted"))
    insert_ds_final.show(false)


    println("=========== deleted records =================")
    val delete_ds = lookUpDS.join(newDS,Seq(join_key),"left_anti")
    val delete_ds_final =
      delete_ds
          .withColumn("current",lit(true))
          .withColumn("effectiveEndDate",current_date())
          .withColumn("flag",lit("Deleted"))
    delete_ds_final.show(false)


  /*  println("=========== retained records =================")
    val retained_ds =
      lookUpDS
        .where("current = 'false' ")
        .join(newDS,Seq(join_key),"left_semi")

    val retained_ds_final =
      retained_ds.withColumn("flag",lit("Retained"))
    retained_ds_final.show(false)*/

    println("=========== upsert/updated records =================")
    val upsert_ds_final =
      lookUpDS.as("left")
        .join(newDS.as("right"),
                col("left.customerId").equalTo(col("right.customerId")),
                "inner")
        .withColumn("flag",
                     when(col("current").equalTo(lit(false)),lit("Retained"))
                    .otherwise(lit("Upsert"))
                   )
          .select("left.*","right.*","flag")
    upsert_ds_final.show(false)

    println("========   retained_ds2 ======================")
    val retained_ds2 = upsert_ds_final
        .where("flag = 'Retained'")
        .select("left.*","flag")
    retained_ds2.show(false)

    println("========   update_ds2 ======================")
    val update_ds2 = upsert_ds_final

      .where("flag = 'Upsert'")
      .select("left.*","flag")
      .withColumn("current",lit(false))
      .withColumn("effectiveEndDate",current_date())
      .withColumn("flag",lit("update|Retained"))
    update_ds2.show(false)


    println("========   update_insert_ds2 ======================")
    val update_insert_ds2 = upsert_ds_final.where("flag = 'Upsert'")
      .select("right.*","flag")
      .withColumn("current",lit(true))
      .withColumn("effectiveEndDate",lit(null))
      .withColumn("flag",lit("insert"))

    update_insert_ds2.show(false)

    println("===========      scd2  =========================================")
    val scd_2 =
       insert_ds_final
         .unionByName(delete_ds_final)
         .unionByName(retained_ds2)
         .unionByName(update_ds2)
         .unionByName(update_insert_ds2)

    scd_2.show(false)

    spark.stop()

  }
}

