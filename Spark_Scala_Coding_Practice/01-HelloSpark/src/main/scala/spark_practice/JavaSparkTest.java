package spark_practice;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JavaSparkTest {
    public static void main(String[] args) {
        String s = "ANEK";
        System.out.println(s.substring(s.length() - 1));

        System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software");
        SparkSession spark ;
        spark = SparkSession.builder()
                .master("local[3]")
                .config("spark.sql.shuffle.partitions", 2)
                .appName("SparkLoaderTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<Row> listOfRows = Arrays.asList(
                                        RowFactory.create(101,"Anek","HR"),
                                        RowFactory.create(102,"Neha","Finance")
                                     );
        StructType schema = new StructType(new StructField[]{
                new StructField("id",   DataTypes.IntegerType,true, Metadata.empty()),
                new StructField("name", DataTypes.StringType,false, Metadata.empty()),
                new StructField("dept", DataTypes.StringType,true, Metadata.empty())
        }
        );
       Dataset<Row> ds= spark.createDataFrame(listOfRows,schema);
        System.out.println("listOfRows  Schema");
        ds.printSchema();
        ds.show();
        System.out.println("listOfRows  DataType \n ");
        for(StructField field : ds.schema().fields() ){
            System.out.println(" field Name : "+field.name());

            System.out.println("  toDDL : "+field.toDDL());

            System.out.println(" dataType Name : "+field.dataType());

            System.out.println(" metadata : "+field.metadata());
            System.out.println(" field.dataType().defaultSize() : "+field.dataType().asNullable());
            System.out.println();
        }








        

       List<Emp> listOfEmps = Arrays.asList( new Emp(101,"Anek","HR") ,
                                          new Emp(102,"Neha","Finance") ) ;


        Dataset<Emp> dsEmp =  spark.createDataset(listOfEmps,Encoders.bean(Emp.class));
        System.out.println("listOfEmps  Schema");
        dsEmp.printSchema();
        //dsEmp.show();

        List<Row> listOfLists = Arrays.asList(
                RowFactory.create(Arrays.asList("fridge,AC,Cooler,Heater")) ,
                RowFactory.create(Arrays.asList("fridge,AC,Cooler,Heater"))
                );

        StructType schemaForLists = new StructType(
                new StructField[]{
                        new StructField("items",DataTypes.createArrayType(DataTypes.StringType),true,Metadata.empty())

                }

        );
        Dataset<Row> dsLists =  spark.createDataFrame(listOfLists,schemaForLists);
        dsLists.printSchema();
        //dsLists.show();
                     // Doesnot work because items column is not part of dsEmp
       // dsEmp.withColumn("items",dsLists.col("items")).show();













        spark.stop();

    }

}

