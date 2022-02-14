package spark_practice
import org.apache.spark.sql.Row
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, IntegerType, StructField, StructType}

object empty_data_set {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C\\Spark_3_Software\\");

    System.setProperty("hadoop.home.dir","C:\\Spark_3_Software\\");
    val spark = SparkSession.builder().master("local[3]")
      .appName("empty_data_set")
      .config("spark.sql.shuffle.partitions",3).getOrCreate()

    import spark.implicits._
/*    val ds  = spark.emptyDataFrame
    val schema = new StructType()
                      .add(StructField("firstName",DataTypes.StringType, true))
                      .add(StructField("lastName",DataTypes.StringType, true))
                      .add(StructField("middleName",DataTypes.StringType, true))

    val ds2= spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    ds.printSchema()
    ds2.show()
    val colSeq = Seq("firstName","lastName","middleName")
    Seq.empty[(String,String,String)].toDF(colSeq:_*).show()*/

    val ds2 = Seq( ("A,B"), ("C,D,E") ).toDS
    //ds2.as[String,String,String].show()


    val ds1 = Seq( ("A,B"), ("C,D,E") ).toDF("c")
    ds1.createOrReplaceTempView("t")
    ds1.withColumn("out",explode(split($"c",","))).show()

    //spark.sql("select regexp_substr('A1,A2,A4','[^,]+', 3, 2)").show

    spark.stop

  }

}
