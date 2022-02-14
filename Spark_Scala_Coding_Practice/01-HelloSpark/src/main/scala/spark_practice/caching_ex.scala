package spark_practice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.storage.StorageLevel

object caching_ex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("caching").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    /*val ds = spark.read.option("inferSchema",true).option("header",true).format("csv").load("data/persons.csv")

    val ds2 = ds.groupBy("person_name").count()
    ds2.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    ds2.show()*/
    /*for(x <- ds2.columns){
      ds2.select("person_name").show()
    }*/
    import spark.implicits._

    Seq( ("2009/11/02"),("2020/11/03") ).toDF("Date").select(
      col("Date"),
      to_date(col("Date"),"yyyy/dd/MM").as("to_date"),
      date_format(to_date(col("Date"),"yyyy/dd/MM"),"dd-MM-yyyy").as("date_format").cast(DateType)
    ).printSchema()


    /*val df = List(1,2,3,4,5,6,7,8,9,10,11).toDF("num")
    df.show()
    val df2 = df.filter("num <= 7")
    df2.show()
    val df3 =  df.except(df2)
    df3.show()

    val ds = spark.read.parquet("./data/emp_partitioned").cache()
    ds.printSchema()
    println("ds.rdd.getNumPartitions :: "+ds.rdd.getNumPartitions)

    println("\n mapPartitionWithIndex :: ")
    ds.rdd.mapPartitionsWithIndex( (index, iterator) => iterator.map(x => (index,x)) ).foreach(println)

    println("\n mapPartitionWithIndex :: 1")
    ds.rdd.mapPartitionsWithIndex( (index,iterator) => {
      iterator.toList.map(x =>  if (index == 1)  {println(x)}  ).toIterator
    }
    ).collect().foreach(println)
*/
    //scala.io.StdIn.readLine()
    //Thread.sleep(10000)

    spark.stop

  }

}
