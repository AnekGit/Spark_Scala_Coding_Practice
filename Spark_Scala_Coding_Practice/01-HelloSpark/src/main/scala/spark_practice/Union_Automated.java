package spark_practice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * created by ANEK on Sunday 11/7/2021 at 1:24 PM
 */

public class Union_Automated {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\Spark_3_Software");
        SparkSession spark ;
        spark = SparkSession.builder()
                .master("local[4]")
                .appName("UnionTest")
                .config("spark.sql.shuffle.partitions","3")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        List<Row> rowList = Arrays.asList(
                RowFactory.create("103","TV",2000,"2018-12-25","2021-03-09","V01") ,
                RowFactory.create("104","LED",200,"2018-12-05","2020-09-17","V02") ,
                RowFactory.create(null,"TV",2000,"2018-12-25","2021-03-09","PERSONAL")

        );
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("id", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("name", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("amount", DataTypes.IntegerType,true, Metadata.empty()),
                        new StructField("sale_date", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("create_time", DataTypes.StringType,true, Metadata.empty()),
                        new StructField("branch", DataTypes.StringType,true, Metadata.empty())
                }

        );
        Dataset<Row> dsSales1 = spark.createDataFrame(rowList,schema);
        System.out.println("dsSales1 ==> ");
        dsSales1.show();

        Dataset<Row> emptyDS = spark.emptyDataFrame().withColumn("id1",lit(null).cast(DataTypes.StringType));
        System.out.println("dsSales1.join(emptyDataFrame,\"id\") ==> ");
        dsSales1.join(emptyDS,col("id").equalTo(col("id1")),"leftanti") .show();




        List<Row> rowList2 = Arrays.asList(
                RowFactory.create(103,"TV",2000,"2018-12-25","2021-03-09","PERSONAL") ,
                RowFactory.create(104,"LED",200,"2018-12-05","2020-09-17","INSTITUTIONAL"),
                RowFactory.create(null,"TV",2000,"2018-12-25","2021-03-09","PERSONAL")

        );
        StructType schema2 = new StructType(
                new StructField[]{
                        new StructField("id", DataTypes.IntegerType,true, Metadata.empty()) ,
                        new StructField("name", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("amount", DataTypes.IntegerType,true, Metadata.empty()),
                        new StructField("sale_date", DataTypes.StringType,true, Metadata.empty()) ,
                        new StructField("create_time", DataTypes.StringType,true, Metadata.empty()),
                        new StructField("business_type", DataTypes.StringType,true, Metadata.empty())
                }

        );
        Dataset<Row> dsSales2 = spark.createDataFrame(rowList2,schema2);
        System.out.println("dsSales2 ==> ");
        dsSales2.show();

        System.out.println("join using column :: id");
        dsSales1.join(dsSales2,"id").show();

        System.out.println("join using column :: seq of columns ");
        List<String> usingColumns = Arrays.asList("id","name","amount");
        Seq<String> usingCols = JavaConverters.iterableAsScalaIterable(usingColumns).toSeq();
        dsSales1.join(dsSales2,usingCols).show();

        System.out.println("join using column :: seq of columns ==>  leftsemi");
        dsSales1.join(dsSales2,usingCols,"leftsemi").show();
        System.out.println("********************************************************************************");

        List<String> ans12 = diff ( Arrays.asList(dsSales1.columns()) , Arrays.asList( dsSales2.columns()) );
        List<String> ans21 = diff ( Arrays.asList(dsSales2.columns()) , Arrays.asList( dsSales1.columns()) );

        System.out.println(" sales1 - sales2 : "+ ans12);
        System.out.println(" sales2 - sales1 : "+ ans21);

        for(String s : ans12) dsSales2 = dsSales2.withColumn(s,lit("Null"));
        for(String s : ans21) dsSales1 = dsSales1.withColumn(s,lit("Null"));

        dsSales1.show();
        dsSales2.show();

        dsSales1.unionByName(dsSales2).show();

    }
    public static List<String> diff(List<String> list1 ,List<String> list2 ){
        List<String> minus = new ArrayList<>();
        list1.stream().forEach(
                e -> {
                    if (!list2.contains(e)) minus.add(e);
                }

        );
        return minus;



    }
}
