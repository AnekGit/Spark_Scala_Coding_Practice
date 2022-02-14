package spark_practice;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.expressions.Window;
import scala.collection.JavaConverters;


import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkLoader {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\winutils");
        SparkSession spark ;
        spark = SparkSession.builder()
                       .master("local[3]")
                       .config("spark.sql.shuffle.partitions",2)
                        //.enableHiveSupport()
                       .appName("SparkLoaderTest")
                       .getOrCreate();
       spark.sparkContext().setLogLevel("WARN");

       Dataset ds = spark.read().option("header","true").option("inferSchema","true").csv("data/my_emp");
       String[] cols = ds.columns();
       for(String col :cols){
          Column column =  ds.col(col).eqNullSafe(ds.col(col));
           System.out.println("Column "+col+" is clear");
       }


    

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
        dsSales = dsSales.withColumn("id", col("id").cast( DataTypes.createDecimalType(10,0)) );
        dsSales = dsSales.withColumn("amount", col("amount").cast( DataTypes.createDecimalType(10,5)) );
        dsSales = dsSales.withColumn("sale_date", col("sale_date").cast( DataTypes.DateType) );
        dsSales = dsSales.withColumn("create_time", col("sale_date").cast( DataTypes.TimestampType) );
        //dsSales = dsSales.withColumn("id_new", lit("null").cast( DataTypes.createDecimalType(10,0))            );

        String tableName = "finance.PRODUCTS_2018_04_01";
        dsSales.printSchema();
        System.out.println(dsSales.schema().toDDL());
        dsSales.show();

        for(StructField field : dsSales.schema().fields() ){
            System.out.println(" field Name : "+field.name());

            System.out.println("  toDDL : "+field.toDDL());

            System.out.println(" dataType Name : "+field.dataType());

            System.out.println(" metadata : "+field.metadata());
            System.out.println(" field.dataType().defaultSize() : "+field.dataType().asNullable());
            System.out.println();
        }
      // saveToDB(dsSales,tableName);




        spark.stop();
    }

    private static void saveToDB(Dataset ds,String tableName){
        ds.write().format("jdbc").mode(SaveMode.Overwrite).
                option("url", "jdbc:oracle:thin:@localhost:1521:AnekDB").
                option("driver", "oracle.jdbc.driver.OracleDriver").
                option("continueBatchOnError","true").
                option("user", "finance").
                option("password", "finance").
                option("dbtable",tableName).
                option("truncate","true").
               /* option("partitionColumn","JOIN_DATE").
                option("lowerBound","2020-06-01").
                option("upperBound","2021-02-01").
                option("numPartitions","3").
                option("dateFormat", "yyyy-MM-dd HH24:MI:SS").
                option("sessionInitStatement",
                        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-MM-dd HH24:MI:SS' ").*/
                        save() ;
    }


   

}
