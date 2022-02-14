package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * created by ANEK on Thursday 9/9/2021 at 12:53 PM 
 */

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder()
      .master("local[3]")
      .appName("streaming_word_count")
      .config("spark.sql.shuffle.partitions",3)
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation","true")
      .getOrCreate()

    val stream_ds = spark.readStream.format("socket")
                    .option("host","localhost").option("port","9999").load()

    stream_ds.printSchema()
    val grp_ds = stream_ds
      .select(explode( split(col("value")," ") ).as("exploded"))
      .groupBy(col("exploded")).count()

    grp_ds
      .writeStream.format("console")
       .option("checkpointLocation","data")
        .outputMode("complete")
        .start()

    
    spark.stop()

  }
}
