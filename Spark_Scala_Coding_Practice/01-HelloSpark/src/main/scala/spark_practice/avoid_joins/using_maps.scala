package spark_practice.avoid_joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.sparkproject.jetty.http.MetaData
object using_maps {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")

    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("using_maps")
      .config("spark.sql.shuffle.partitions","3")
      //.enableHiveSupport()
      .getOrCreate()

    val options = Map("header"     -> "true",
                      "sep"        -> ",",
                      "inferSchema"-> "true")
    val inputDS =  spark.read.options(options).csv("./data/inputFileCustAccounts.csv")
    inputDS.show(false)

    val lookUpDS =  spark.read.options(options).csv("./data/lookUpFileCustAccountDetails.csv")
    lookUpDS.show(false)

    inputDS.createOrReplaceTempView("inputFileCustAccounts")
    lookUpDS.createOrReplaceTempView("lookUpFileCustAccountDetails")

    val stateCountryDS =  spark.read.options(options).csv("./data/stateCountryMapping.csv")
    stateCountryDS.show(false)

    val countryMap = stateCountryDS.select("state","country")
    .rdd.map(e => (e.getString(0) -> e.getString(1) ) ).collectAsMap
    println(countryMap)

    val mapToCol = typedLit(countryMap)

    val inputCountryDS = inputDS.withColumn("country_state",mapToCol( col("state_code")  ) )
    val lookUpCountryDS = lookUpDS.withColumn("country_state",mapToCol( col("state_code_lkp") ) )
    inputCountryDS.createOrReplaceTempView("inputCountryDS")
    lookUpCountryDS.createOrReplaceTempView("lookUpCountryDS")

    println(s"with state mapping ${}")
    spark.sql(
      """
        |select a.*,b.* from  inputFileCustAccounts a INNER JOIN  lookUpFileCustAccountDetails b
        |ON
        |a.account = b.account_lkp AND a.state_code = b.state_code_lkp
        |""".stripMargin).show(false)

    println(s"country mapping ds ${}")
    inputCountryDS.show()
    lookUpCountryDS.show()

    spark.sql(
      """
        |select a.*,b.* from  inputCountryDS a INNER JOIN  lookUpCountryDS b
        |ON
        |a.account = b.account_lkp AND a.country_state = b.country_state
        |""".stripMargin).show(false)


    val dateSchema = StructType(
      List(
        StructField("some_date",DataTypes.DateType,true)
      )
    )
    val someDateDS =  spark.read.options(options).schema(dateSchema).csv("./data/somedate.csv")
    someDateDS.printSchema()
    someDateDS.show(false)

    someDateDS.withColumn("days_to_remain",
                                  abs(datediff(  col("some_date"),lit("2021-12-31") ))

    ).show(false)

    import spark.implicits._

    // METHOD 2 ::
    val arr : List[Long] = List(1L,2L,3L)
    val dsLong = spark.createDataset(arr)
    
    println(s"dsLong num partitions :: ${dsLong.rdd.getNumPartitions}")
    dsLong.show()

    // METHOD 3 ::
    val acc = Account("101","Anek","1000")
    val accList = Seq(acc)
    val dsAcc = spark.createDataset(spark.sparkContext.parallelize(accList,2))
    
    println(s"dsLong num partitions :: ${dsAcc.rdd.getNumPartitions}")
    dsAcc.printSchema()
    dsAcc.show()


    spark.stop
  }
  case class Account(account_no : String ,name :String ,salary : String )
}
