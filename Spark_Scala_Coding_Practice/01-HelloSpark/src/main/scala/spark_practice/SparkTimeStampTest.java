package spark_practice;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.collection.Seq;
import scala.xml.MetaData;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class SparkTimeStampTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTimestamp Test").setMaster("local[3]").set(" spark.sql" +
                ".legacy.utcTimestampFunc.enabled", String.valueOf(Boolean.TRUE));
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        String table_name = "order_time_stamp_test";
        getJDBCOptions(table_name);
       // Dataset<Row> ds = readFromDB(spark,table_name);
       // ds.printSchema();

     //   System.out.println("here is the count : :"+ds.count());
               
        List<Row> listOfRows = Arrays.asList(

                RowFactory.create("2021-03-13"),
                RowFactory.create("2021-03-14")

        );
        StructType schema = DataTypes.createStructType(
                Arrays.asList(
                      new StructField("ORDER_DATE",DataTypes.StringType,false, Metadata.empty())

                )
        );

        Dataset<Row> orderDS  = spark.createDataFrame(listOfRows,schema);
     
        orderDS = orderDS.withColumn("ORDER_DATE",to_timestamp( col("ORDER_DATE"),"yyyy-MM-dd" ) );
        orderDS = orderDS.withColumn("CRE_DATE",to_timestamp(lit("2020-04-05"),"yyyy-MM-dd" )  ) ;
        orderDS = orderDS.withColumn("ORDER_UNIX_TIMESTAMP",unix_timestamp()  ) ;
        orderDS = orderDS.withColumn("CURRENT_TIMESTAMP",current_timestamp() ) ;
        orderDS = orderDS.withColumn("CURRENT_TIMESTAMP_TRUNC",trunc(current_timestamp(),"yy" ) ) ;
        orderDS = orderDS.withColumn("FROM_UNIX_TIMESTAMP",
                to_timestamp( from_unixtime( col("ORDER_UNIX_TIMESTAMP") ) ) );

        System.out.println("check :: ");
        orderDS.printSchema();
        orderDS.show();

        List<Tuple2> list = Arrays.asList(
                                           new Tuple2(1, Timestamp.valueOf("2014-01-01 23:00:01") ),
                                           new Tuple2(1, Timestamp.valueOf("2014-11-30 12:40:32") ),
                                           new Tuple2(2, Timestamp.valueOf("2016-12-29 09:54:00") ),
                                           new Tuple2(2, Timestamp.valueOf("2016-05-09 10:12:43") )    );
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        JavaRDD<Tuple2> javaRDD = jsc.parallelize(list);
        System.out.println(javaRDD.collect());

        StructType schemaTuple = DataTypes.createStructType(
                Arrays.asList(
                    new StructField("id",DataTypes.IntegerType,false,new MetadataBuilder().build()) ,
                        new StructField("id",DataTypes.TimestampType,false,new MetadataBuilder().build())
                )

        );
      // spark.createDataFrame(list, Encoders.bean(Tuple2.class)) ;


        try{
           // writeToDB(orderDS,table_name);
       }catch (Exception e){
            String err = e.getMessage();
            System.out.println("Err Msg :: "+err);
            if(err.equals("Unrecognized SQL type -102"))
                System.out.println("Data has been written correctly : no need to throw exception");
        }
        spark.stop();
    }

    private static void writeToDB(Dataset ds, String tableName){
        ds.write().format("jdbc").mode(SaveMode.Overwrite).options(getJDBCOptions(tableName)).save();
    }
    private static Dataset<Row> readFromDB(SparkSession spark,String tableName){
       return  spark.read().format("jdbc").options(getJDBCOptions(tableName)).load();
    }

    private static Map<String,String> getJDBCOptions(String tableName){
        Map<String,String> map = new HashMap<>();
        map.put("url", "jdbc:oracle:thin:@localhost:1521:AnekDB");
        map.put("driver", "oracle.jdbc.driver.OracleDriver");
        map.put("user", "finance");
        map.put("password", "finance");
        map.put("dbtable",tableName);
        map.put("truncate","true");
      //  map.put("timestampFormat", "yyyy-MM-dd hh:mm:ss");
        System.out.println("Map ::  url - > "+map.get("url"));
        return map;

    }

}
