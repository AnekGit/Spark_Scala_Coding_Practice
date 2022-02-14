package danske_it

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.io.Directory

object FindLatestRecord {
  def main(args : Array[String]) :Unit = {

    val spark = SparkSession.builder().config("spark.sql.shuffle.partitions","2")
      .master("local[3]").appName("LatestRecord").getOrCreate()

    import spark.implicits._
    val data1 = Seq(
      (100,"Anek",10,3,"2020-07-08") ,(103,"Neha",500,3,"2020-07-04")
    )

    val data2 = Seq(
      (100,"Anek",10,2,"2020-07-09"),(101,"Vicky",100,2,"2020-07-07"),(103,"Neha",100,3,"2020-07-06")
    )

   
    val rdd1 = spark.sparkContext.parallelize(data1)
    println("Patitioner :  "+rdd1.partitioner)
    
    val df1 = rdd1.toDF("id","name","salary","version","join_date")

    df1.write.format("csv").mode(SaveMode.Overwrite).option("header","true").save("ingest1")

    var ds1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("ingest1")

    ds1 = ds1.withColumn("join_date",to_date($"join_date","yyyy-MM-dd") )



    val rdd2 = spark.sparkContext.parallelize(data2)
    val df2  = rdd2.toDF("id","name","salary","version","join_date")

    df2.write.format("csv").mode(SaveMode.Overwrite).option("header","true").save("ingest2")

    var ds2 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("ingest2")
    ds2 = ds2.withColumn("join_date",to_date($"join_date","yyyy-MM-dd") )
    
    ds1.show
    ds1.printSchema
    ds2.show
    ds2.printSchema

    /*
    * Finding the latest record
    * */
    val windowsSpec = Window.partitionBy("id").orderBy(col("join_date").desc)

    val add1 = ds1.union(ds2)
             add1.withColumn("retain",dense_rank().over(windowsSpec))
                 .where("retain == 1")
                 .drop("retain").show

    /*
  * Suppose you have df with zero records and you have to write it to the File for preserving the schema
  * */



    // Does not work out   :=> ds1.drop("id","name","salary","version","join_date").write.csv("ingest_csv")

    // Conclusion : If record count is zero, still spark write data to file



    //Files.deleteIfExists(Paths.get("ingest_csv"))
    val directory = new Directory(new File("ingest_csv"))
    directory.deleteRecursively()

    ds1.join(Seq(1).toDF("id_1"), $"id" <=> $"id_1" )  // here count is 0
       .write
       .option("header","true")
      // .option("inferSchema","true")
       .csv("ingest_csv")
    val ds3 =spark.read.format("csv").option("header","true").option("inferSchema","true").load("ingest_csv")

    // Conclusion : If record count is zero, still spark write data to file
   

    ds3.show
    ds3.printSchema
    ds3.count

    spark.stop



  }

}
