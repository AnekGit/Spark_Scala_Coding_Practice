package spark_practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/*case object OracleDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.startsWith("jdbc:oracle")
  override def getCatalystType(
                                sqlType: Int, typeName: String, size: Int,
                                md: MetadataBuilder): Option[DataType] = {
    // Handle NUMBER fields that have no precision/scale in special way
    // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
    // For more details, please see
    // https://github.com/apache/spark/pull/8780#issuecomment-145598968
    // and
    // https://github.com/apache/spark/pull/8780#issuecomment-144541760
    if (sqlType == Types.NUMBERIC && size == 0) {
      // This is sub-optimal as we have to pick a precision/scale in advance whereas the data
      // in Oracle is allowed to have different precision/scale for each value.
      Some(DecimalType(38, 10))
    } else {
      None
    }
  }*/
object RowEx {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software\\")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("RowEx")
      // .enableHiveSupport()
      //.config("spark.sql.shuffle.partitions",2)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
   /* val s1 = "\\\u0001";
    val s2 = "\u0001";
    println(s1)
    println(s2)


    */
    import spark.implicits._

    var df = spark.read.format("jdbc").
      option("url", "jdbc:oracle:thin:@localhost:1521:AnekDB").
      option("driver", "oracle.jdbc.driver.OracleDriver").
      option("continueBatchOnError","true").
      option("user", "finance").
      option("password", "finance").
      option("dbtable","finance.emp_test").load();


      df = df.withColumn("decimal",lit("null").cast(DataTypes.createDecimalType(38,9)))
      //df = df.withColumn("string",lit("null").cast(DataTypes.createStructField()))
      //df = df.withColumn("new_added",col("new_added").cast(DataTypes.createDecimalType(38,9)))

    df.printSchema()
    df.show

    val dfDate = Seq(
                      ("20201231"),
                      ("20210131")
                    ).toDF("input_timestamp")

    val fields = dfDate.schema.fields
    println("dataType : :"+fields(0).toString())

    var dsDate = dfDate.withColumn("input_timestamp",col("input_timestamp").cast(DataTypes.LongType))

    val fields11 = dsDate.schema.fields
    println("dataType long: :"+fields11(0).dataType)

    if(fields11(0).dataType.equals(DataTypes.LongType)){
                                                     println("yes match "+DataTypes.LongType)
    }


    dsDate =  dsDate.withColumn("input_timestamp",
      to_date(col("input_timestamp").cast(DataTypes.StringType),"yyyyMMdd")  )

    dsDate.printSchema()
    val fields1 = dsDate.schema.fields
    println("dataType : :"+fields1(0).dataType)
    dsDate.show()

    println("timestamp type ")
    dsDate.withColumn("input_timestamp",col("input_timestamp").cast(DataTypes.TimestampType) )
      .show(true)
    dsDate.withColumn("input_timestamp",to_timestamp(col("input_timestamp"),"yyyyMMdd"))
      .show(false)















      val data1 = Seq (
           (1,"Anek") ,
           (2,null),
        (101,null)
         ).toDF("id","value")

      val data2 = Seq(
        (1,"Anek"),
        (2,null),
        (3,null)
        ).toDF("id","value")

       data1.createOrReplaceTempView("oracle_data")
       data2.createOrReplaceTempView("hive_data")

      println("sql minus")
      //spark.sql("select id,value from oracle_data minus select id,value from hive_data").show

       data1.join(data2,data1.col("value").equalTo(data2.col("value"))).show
       println("with nullEqSafe")
     //  data1.join(data2,data1.col("value").eqNullSafe(data2.col("value"))).show

      println("data1 except data2")
     // data1.except(data2).show

      println("data2 except data1")
     // data2.except(data1).show

      // spark.sql("select stack(2,1,2,3)").show



     spark.stop()
  }

}






/**

spark.sparkContext.parallelize(
      Seq("Anek","2020/05/05")
    ).map(e => e.length).foreach(println)

    val myrow2 = Seq(
      Row("123","NULL","MALE"),
      Row("124", "02/01/2021","FEMALE"),
      Row("125", "03/01/2020","MALE"),
      Row("126", "04/01/2020","FEMALE"),
      Row("127", "05/01/2020","MALE"),
      Row("128", "06/01/2020","FEMALE")


    )
    val myrow = Seq(
      Row("123","MALE"),
      Row("124","FEMALE"),
      Row("125","MALE"),
      Row("126","FEMALE"),
      Row("127","MALE"),
      Row("128","FEMALE")
    )
    val myRdd = spark.sparkContext.parallelize(myrow2)

    // till now it is RDD and we have to make the Dataframe out of it . Attach the schema
    val mySchema2 = StructType(
      Array(
        StructField("id",StringType),
        StructField("join_date",StringType),
        StructField("category",StringType)
      )
    )

    val mySchema = StructType(
      Array(
        StructField("id",StringType),
        StructField("category",StringType)
      )
    )


    var myDf = spark.createDataFrame(myRdd,mySchema2)
    myDf.printSchema
    myDf.show
    myDf = myDf.withColumn("join_date", to_timestamp(col("join_date"),"MM/dd/yyyy")  )

   /* val myDf2 = myDf1.withColumn("time_stamp",lit(current_timestamp()))
 */
    myDf.printSchema()
    myDf.show(truncate = false)
    myDf.createOrReplaceTempView("my_emp")

    println("--------  spark-sql  -------------")

    /*myDf.repartition(1).write
      .format("csv")
      .option("header","true")
      .mode("overwrite")
      .option("sep","@")
      .save("spark-warehouse/my_emp")*/

    //spark.catalog.listDatabases().show(false)
    //spark.catalog.listTables().show
   // spark.sql("describe my_emp")
    //spark.read.table("my_emp").show()
    val sql =  "my_emp"
    //val pushDownQuery = sql+" as t where t.CATEGORY='MALE'"
    val pushDownQuery = "("+sql+")"


    val df = spark.read.
      option("url", "jdbc:oracle:thin:@localhost:1521:AnekDB").
      option("driver", "oracle.jdbc.driver.OracleDriver").
      option("continueBatchOnError","true").
      option("user", "finance").
      option("password", "finance").
      option("dbtable","finance.my_emp").
      option("partitionColumn","JOIN_DATE").
      option("lowerBound","2020-06-01").
      option("upperBound","2021-02-01").
      option("numPartitions","3").
      option("dateFormat", "yyyy-MM-dd HH24:MI:SS").
      option("sessionInitStatement",
                                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-MM-dd HH24:MI:SS' ").

      format("jdbc").
      load()

      df.write.option("header","true").option("inferSchema","true").csv("data/my_emp")


/*
      option("partitionColumn","ID").
      option("lowerBound","121").
      option("upperBound","130").
      option("numPartitions","3").
      format("jdbc").
 */

      //df.write.option("header","true").option("inferSchema","true").csv("data/my_emp")

        /*spark.sql("select id,category,join_date from my_emp DISTRIBUTE BY join_date SORT BY category").show
    spark.sql("select id,category,join_date from my_emp CLUSTER BY join_date")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("store")
      //.show*/



   myDf2.repartition(col("join_Date")).sortWithinPartitions("id")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("join_date")
      //.bucketBy(2,"category")
      .parquet("store")




    spark.stop














 */