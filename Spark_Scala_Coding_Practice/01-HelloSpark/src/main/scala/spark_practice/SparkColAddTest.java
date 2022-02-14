package spark_practice;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * created by ANEK on Wednesday 8/25/2021 at 5:06 PM
 */

public class SparkColAddTest {
    public static void main(String[] args) {

        System.out.println("ANEKSINGH".substring(0,3));
        System.setProperty("hadoop.home.dir","C:\\Spark_3_Software");
        SparkSession spark ;
        spark = SparkSession.builder()
                .master("local[3]")
                .config("spark.sql.shuffle.partitions",2)
                .appName("SparkLoaderTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");


        List<Row> rowList = Arrays.asList(
                RowFactory.create(103,"TV",2000,"2018-12-25","2021-03-09") ,
                RowFactory.create(104,"LED",200,"2018-12-05","2020-09-17")

        );
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("id", DataTypes.IntegerType,false, Metadata.empty()) ,
                        new StructField("name", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("amount", DataTypes.IntegerType,true, Metadata.empty()),
                        new StructField("sale_date", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("create_time", DataTypes.StringType,true, Metadata.empty())
                }

        );
        List<Row> rows2 = Arrays.asList(
                RowFactory.create(1,"Anek") ,RowFactory.create(1,"Vicky")
        );
        StructType schema2 = new StructType()
                .add( new StructField("id", DataTypes.IntegerType,false, Metadata.empty()) )
                .add( new StructField("name", DataTypes.StringType,true, Metadata.empty()) );

        Dataset<Row> ds = spark.createDataFrame(rows2,schema2);
        System.out.println("+++++++++++++++++++++++++++++++++++");
        ds.show();
        ds.groupBy("id")
                .agg(
                        collect_list("name")
        ).show(false);
        System.out.println("+++++++++++++++++++++++++++++++++++");


        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add( new StructField("id", DataTypes.IntegerType,false, Metadata.empty()));
        structFieldList.add( new StructField("name", DataTypes.StringType,true, Metadata.empty()));
        structFieldList.add( new StructField("amount", DataTypes.IntegerType,true, Metadata.empty()));
        structFieldList.add( new StructField("sale_date", DataTypes.StringType,true, Metadata.empty()) );
        structFieldList.add( new StructField("create_time", DataTypes.StringType,true, Metadata.empty()));


        StructType structType = DataTypes.createStructType( structFieldList );

        //spark.sparkContext().parallelize(rowList);
        Seq<Row> seqRow = JavaConverters.asScalaIteratorConverter(rowList.iterator()).asScala().toSeq();

        //RDD<Row> parallelize = spark.sparkContext().parallelize(seqRow,2,);
        System.out.println("seqRow : : \n "+seqRow);




        Dataset<Row> dsSales = spark.createDataFrame(rowList,structType);

        dsSales.withColumn("add",col("id").$plus(col("amount")) )
                .withColumn("add_more ",col("add").$plus(lit("10").cast(DataTypes.IntegerType))).show();

      //  dsSales.printSchema();

    }
}
