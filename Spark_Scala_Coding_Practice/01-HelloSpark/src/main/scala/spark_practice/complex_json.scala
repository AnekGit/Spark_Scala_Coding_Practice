package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complex_json {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("Json_Ex").getOrCreate()

    val df = spark.read.option("multiline","true").json("data/sample.json")
    import spark.implicits._

    df.printSchema()

    val df_1 =
      df
      .withColumn("accounts",explode( col("accounting") )  )
      .withColumn("sale",    explode( col("sales") )  )
      .drop("accounting","sales").select($"accounts.*",$"sale.*")


    df_1.show



   /* val df_with =
      df
        .withColumn("Emp_Name",$"emp1.EmpName")
        .withColumn("Emp_Age",$"emp1.EmpAge")
        .withColumn("Emp_Gender",$"emp1.EmpGender")
        .withColumn("Emp_Dept",$"emp1.EmpDept")
        .withColumn("EmpDesg",$"emp1.EmpDesg")




    df_with.printSchema
    df_with.show*/

    spark.stop()

  }
}
