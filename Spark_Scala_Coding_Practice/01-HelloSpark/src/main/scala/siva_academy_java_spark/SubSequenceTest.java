package siva_academy_java_spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

public class SubSequenceTest {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Spark_3_Software");
        SparkSession spark ;
        spark = SparkSession.builder()
                       .master("local[3]")
                       .config("spark.sql.shuffle.partitions",2)
                       .appName("SparkLoaderTest")
                       .getOrCreate();
       spark.sparkContext().setLogLevel("WARN");

       List<Row> rows = Arrays.asList(
               RowFactory.create(1),
               RowFactory.create(2), RowFactory.create(3),RowFactory.create(5),RowFactory.create(7),
               RowFactory.create(8),RowFactory.create(9),RowFactory.create(11),RowFactory.create(12),
               RowFactory.create(15),RowFactory.create(16),RowFactory.create(20)
       );
       StructType type = new StructType().add(
               new StructField("c",DataTypes.IntegerType,false,Metadata.empty())
       );
       System.out.println("creating dataframe");
       Dataset<Row> ds = spark.createDataFrame(rows,type);
      // ds.show();

       // select all columns in spark java


       List<Test> list = Arrays.asList(
               Test.of(1),
               Test.of(2), Test.of(3),Test.of(5),Test.of(7),
               Test.of(8),Test.of(9),Test.of(11),Test.of(12),
               Test.of(15),Test.of(16),Test.of(20)
       );
       Dataset<Test> testds = spark.createDataset(list,Encoders.bean(Test.class));
       System.out.println("creating dataset");
      // testds.show();


       // 3rd way of creating dataset
        Seq<Test> seq = (Seq<Test>) convertListToSeq(list);
        Dataset<Test> seqds = spark.createDataset(seq,Encoders.bean(Test.class));
        System.out.println("seq dataset");
        //seqds.show();

        ds = ds.withColumn("rownum",monotonically_increasing_id().plus(1))
                .withColumn("c-rownum",col("c").minus(col("rownum")));


        ds= ds.groupBy("c-rownum")
                .agg(   min(col("c")).as("startRange"),   max(col("c")).as("endRange")  )
                .withColumn("start - end",
                                                concat_ws("<->",col("startRange"),col("endRange")))
                .orderBy(col("c-rownum")) ;

        ds.select("*").show();



    }
    public static  Seq<? extends Object> convertListToSeq(List<? extends Object> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }
    public static class  Test implements Serializable {Integer i;

        public Test(Integer i) {
            this.i = i;
        }
        public Test(){ }

        public static Test of(Integer i ){
                 return new Test(i);
        }

        public Integer getI() {
            return i;
        }

        public void setI(Integer i) {
            this.i = i;
        }
    }
}
