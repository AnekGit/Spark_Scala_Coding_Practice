package spark_practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

public class FindFaultyRecord {

    public static void main(String[] args) {

    //SparkSession sparkSession = SparkSession.builder().appName("FindFaultyRecord").master("local[*]").getOrCreate();

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = javaSparkContext.textFile("F:\\Spark_workspace\\SparkProgrammingInScala-master\\SCD2\\data\\word.txt");

        /**Splitting the word with space*/
        textFile = textFile
                .flatMap( line -> Arrays.asList( line.split(" ") ).iterator() );

        System.out.println(textFile.collect());
        /**Pair the word with count*/
        JavaPairRDD<String, Integer> mapToPair = textFile.mapToPair(w -> new Tuple2<>(w, 1));

        /**Reduce the pair with key and add count*/
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((sum, wc2) -> sum + wc2);

        System.out.println(reduceByKey.collectAsMap());
        javaSparkContext.close();
    }


}
