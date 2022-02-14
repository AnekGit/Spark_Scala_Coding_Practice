
package spark_practice.interview_questions

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


object interview {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software");
    val spark = SparkSession.builder().master("local[3]").appName("ledger_bal_impl")
      .config("spark.sql.shuffle.partitions",3).getOrCreate()
    import spark.implicits._
    val currentDS = Seq(
      (100,"CR",100,"2022-01-01"),
      (100,"CR",2000,"2022-01-02"),
      (100,"DR",100,"2022-01-03"),
      (101,"DR",1000,"2022-01-01"),
      (101,"DR",1000,"2022-01-02"),
      (101,"CR",20000,"2022-01-03")
    ).toDF("acct_num","txn_type","amount","txn_date")
    currentDS.show()
    val ds = currentDS.withColumn("new_amount",
          when(
            col("txn_type").equalTo(lit("DR")),col("amount").multiply("-1")
          ).otherwise(col("amount"))  )
    ds.show()
    ds
      .groupBy(col("acct_num"))
      .agg(sum(col("new_amount")).as("ledger_balance"))
      .show()
    spark.stop()

  }
}

