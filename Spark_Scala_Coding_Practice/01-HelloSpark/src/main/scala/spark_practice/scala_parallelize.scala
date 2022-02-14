package spark_practice
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.types.{DateType, IntegerType}
case class Persons(id:Int ,name :String, birth_date:Date)
case class MaxBirthDate(maxBirthDate : Date)

object scala_parallelize {
  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software")
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Json_Ex")
      .config("hadoop.home.dir", "C:\\Spark_3_Software")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println(spark.sparkContext.parallelize(Seq(1, 2), 2).glom().collect())

    val list : List[Int]= List(1,2)
    val rdd : RDD[Int] = spark.sparkContext.parallelize(list,2)

    println("scala javaRDD.collect : -"+spark.sparkContext.parallelize(list,2).toJavaRDD().collect())

    println("scala rdd.collect :- "+ spark.sparkContext.parallelize(list,2).collect())



    val d = Seq("1", "2")
    import spark.implicits._
    //StructType(Array(StructField("id", false, MetaData.Empty))))
    val dd = d.toDF("list")
    dd.show()




   // val d1 = Seq(Row(1, "Anek", "1991-07-05"), Row(2, "Dost", "1991-10-10"))
    val d1 = List( Row(1, "Anek", java.sql.Date.valueOf("1991-07-05") ),
                   Row(2, "Dost", java.sql.Date.valueOf("1991-10-10") ) )
    /*
       val d1 = List(Row(1, "Anek", "1991-07-05"), Row(2, "Dost", "1991-10-10")).toDF    // FAIL
    */
    val schema = StructType(
      List(
        StructField("id", DataTypes.IntegerType, false, Metadata.empty),
        StructField("name", DataTypes.StringType, false, Metadata.empty),
        StructField("join_date", DataTypes.DateType, false, Metadata.empty)

      )

    )
    println("spark scala df 1")
    val personsDF1 : Dataset[Row] = spark.createDataFrame(spark.sparkContext.parallelize(d1).toJavaRDD(),schema)
    personsDF1.printSchema()
    personsDF1.show


    println("spark scala df 2")
    val personsDF2 : Dataset[Row] = spark.createDataFrame(spark.sparkContext.parallelize(d1),schema)
    personsDF2.printSchema()
    personsDF2.show

    /*
    DOES NOT WORK 

    println("spark scala df 3")
    val personsDF3 : Dataset[Row] = spark.createDataFrame(d1,schema)
    personsDF3.printSchema()
    personsDF3.show

    */                                java.sql.Date.valueOf(java.time.LocalDate.now())

    val personDS :Dataset[Persons]= Seq(
                                      Persons(1, "Anek", Date.valueOf("1991-07-05") ),
                                      Persons(2, "Dost", Date.valueOf("1991-10-10") )
                                      ).toDS()

    personDS.printSchema()

    val maxBirth : Dataset[Row] = personDS.agg(  max(col("birth_date") ) ).cache()
    
     val mDS : Dataset[MaxBirthDate] =  maxBirth.as[MaxBirthDate]
    //val maxBirthDate : Dataset[MaxBirthDate] = personDS.agg( max(col("birth_date") ) ).as[MaxBirthDate]


    maxBirth.printSchema()
    maxBirth.show()

    println("mDS")
    mDS.printSchema
    mDS.show
    
    //maxBirthDate.printSchema()
   // maxBirthDate.show()
    //d1.toDS()
    //val rdd : RDD[Row] = spark.sparkContext.parallelize(d1, 2)
   // spark.createDataFrame(rdd,schema)
    //val df1 = spark.createDataFrame(rdd, schema)

    //df.sa



  }
}
