package spark_practice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * created by ANEK on Thursday 12/30/2021 at 11:44 AM 
 */
object handling_null {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("handling_null").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.parallelize(
      Seq(
        Row(1,"Anek",java.sql.Date.valueOf("2021-12-29"),30),
        Row(2,"Dev",java.sql.Date.valueOf("2021-05-14"),14),
        Row(3,"Vicky",java.sql.Date.valueOf("2021-07-05"),30)
      )
    )
    val schema = StructType(
      Seq(
        StructField("id",IntegerType,false),StructField("name",StringType,false),
        StructField("dob",DateType,false),StructField("age",IntegerType,false)
      )
    )
    val ds1 = spark.createDataFrame(data,schema)
    ds1.show()

    val ds = spark.read
                  .options(Map("inferSchema"->"true","header"->"true","delimiter"->","))
                  .format("csv")
                  .load("./data/customer_account/customer_with_null.csv")
                  .withColumn("trans_date",$"Date".cast(DateType))
                  .drop("Date")

    println("using nvl function ")
    val nvlDS = ds.withColumn("nvl_col",expr("nvl(transaction_type,'TT')"))

    val nvl2DS = nvlDS
      .withColumn("nvl2_col",expr("nvl2(transaction_type,transaction_type,'it is null')"))


    val dd = nvl2DS.withColumn("na_nvl",nanvl(col("transaction_type"),lit("no value"))).show



    println("Transaction_Type or card_type is null")
    ds.printSchema()
    ds.where("transaction_type is null or card_type is null ").show()
    ds.where(col("transaction_type").isNull.or(col("card_type").isNull)).show()

    println("filling Amount and Customer_no field with -99999 & filling null values with NA")
    ds.na.fill(-99999.0)
          .na.fill("NA")
          .show(false)


    println("**********  drop all null values ***********")
    ds.na.drop().show()

    println(s"ds.schema ")
    val maps = ds.schema.fields.map(e => Map(e.name -> e.dataType))


    val stringColumns= ds.schema.fields.filter(_.dataType.isInstanceOf[StringType]).map(_.name).toSeq
    val dateColumns = ds.schema.fields.filter(_.dataType.isInstanceOf[DateType]).map(_.name).toSeq
    val longColumns = ds.schema.fields.filter(e => e.dataType.isInstanceOf[LongType]).map(_.name).toSeq
    val integerColumns = ds.schema.fields.filter(_.dataType.isInstanceOf[IntegerType]).map(_.name).toSeq
    val doubleColumns =  ds.schema.fields.filter(_.dataType.isInstanceOf[DoubleType]).map(_.name).toSeq
    val naFillMap = Map("NA"->stringColumns,"null"->dateColumns,"0l"->longColumns,"0"->integerColumns)
    

    println(s"STRING columns =================================================> ")
    println(stringColumns)


    stringColumns.foreach(println)

    println(s"DATE columns ===================================================> ")
    println(dateColumns)
    dateColumns.foreach(println)

    println(s"IntegerType columns ============================================> ")
    println(integerColumns)
    integerColumns.foreach(println)

    var s = dataTypeMatchExpr(ds)
    println(s"s is ${s}")
    println(s"Fill Map of  columns with default values  =================================================> ")
    ds.printSchema()
    ds
      .na.fill("NA", stringColumns)
      .na.fill(0L, longColumns)
      .na.fill(0,  integerColumns)
      .na.fill("2020-01-01",  dateColumns)
      .na.fill(0.00000 ,doubleColumns).show()

   /* println(s"NA isInstanceOf :: ${"NA".isInstanceOf[StringType]}")
    println(s"0L isInstanceOf :: ${0L.isInstanceOf[LongType]}")
    println(s"0 isInstanceOf :: ${0.isInstanceOf[IntegerType]}")
    println(s"0.000 isInstanceOf :: ${0.000.isInstanceOf[DoubleType]}")
    println(s"2020-01-01 isInstanceOf :: ${"2020-01-01".isInstanceOf[DateType]}")
*/

    // SQL Expressions
    println("SQL Expressions :: ")
    ds.selectExpr(
      "Customer_No",
         "Category",
      "Transaction_Type",
      "ifnull(Category,Transaction_Type) as ifnull",
      "nullif(Category,Transaction_Type) as nullif",
      "nvl2(Category,Transaction_Type,'Unknown') as nvl2"
    ) .show




    spark.stop

  }
  def dataTypeMatchExpr(ds: Dataset[Row]) : String = {
    val dataTypeMatch = ds.schema.fields.map(_.dataType).toList
    val dataMatchExpr  = dataTypeMatch.foreach { e => {
      e match {
        case StringType =>  Some("NA")
        case DateType =>  Some("date_value")
        case IntegerType|DoubleType|FloatType =>  Some(-9999)
        case BooleanType =>  Some(false)
        case _ =>  None

      }

    }  //lambda

    }//foreach
    dataMatchExpr.toString
  }

}
