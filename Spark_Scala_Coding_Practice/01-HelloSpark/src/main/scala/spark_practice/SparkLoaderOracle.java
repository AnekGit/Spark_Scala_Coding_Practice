package spark_practice;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Function2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkLoaderOracle {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Spark_3_Software");
        SparkSession spark ;
        spark = SparkSession.builder()
                       .master("local[4]")
                       //.config("spark.sql.shuffle.partitions",2)
                       .appName("SparkLoaderTest")
                       .config("spark.sql.shuffle.partitions","3")
                       .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
/*
        Map<String,String> option = new HashMap<>();
        option.put("url", "jdbc:oracle:thin:@localhost:1521:AnekDB");
        option.put("driver", "oracle.jdbc.driver.OracleDriver");
        option.put("user", "finance");
        option.put("password", "finance");

        // option.put("query","select * from employee where employee.EMP_ENAME = 'ANEK' ");
        // schema.tableName

        option.put("dbtable","employee");
        option.put("numPartitions","1");
        option.put("partitionColumn","EMP_DEPTNO");
        option.put("lowerBound","0");
        option.put("upperBound","10000");

        Dataset<Row> ds = spark.read().format("jdbc").options(option).load();
        System.out.println(ds.rdd().getNumPartitions());

        //partition wise count
        System.out.println( ds.groupBy(spark_partition_id()).count() );
        ds.show();
        //ds= ds.withColumn("partitionColHash",functions.sha1( col("EMP_ENAME") ) );
        ds= ds.withColumn("partitionColHash",monotonically_increasing_id() );
        ds =ds.withColumn("partitionColIndex",
                functions.pmod( col("partitionColHash"),lit("4") ).cast(DataTypes.IntegerType) );
        ds.show();
*/
/*
        ds.foreachPartition(new ForeachPartitionFunction<Row>() {
            public void call(Iterator<Row> t) throws Exception {
                while (t.hasNext()){

                    Row row = t.next();
                    System.out.println(row.toString());
                }
            }
        });
*//*

    */
/*  ds.rdd().mapPartitionsWithIndex( (i,it) -> {
          it.map {x -> (index,x)}
      } ).foreach(x -> System.out.println(x));
  */


/*
       df1.rdd.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
          .toDF("partition_number","number_of_records")
          .show

      base.mapPartitionsWithIndex((index, iterator) => {
      iterator.map { x => (index, x) }
      }).foreach { x => println(x) }

      *//*

                                                        // ds.cache();
                                                        //ds.write().parquet("./data/employee");
                                                        // write to db
       */
        Map<String,String> optionWrite = new HashMap<>();
        optionWrite.put("url", "jdbc:oracle:thin:@localhost:1521:AnekDB");
        optionWrite.put("driver", "oracle.jdbc.driver.OracleDriver");
        optionWrite.put("user", "finance");
        optionWrite.put("password", "finance");
        // option.put("query","select * from employee where employee.EMP_ENAME = 'ANEK' ");
        // schema.tableName
        optionWrite.put("dbtable","employee");
        System.out.println("Writing DS to Oracle ");


        Dataset<Row> dsEmp = spark.read().parquet("./data/employee/");
        System.out.println("After loading parquet from /data location getNumPartitions "+dsEmp.rdd().getNumPartitions());

        System.out.println("After repartition parquet from /data location "+dsEmp.rdd().getNumPartitions());
                   dsEmp.rdd().getNumPartitions();
        int seed = 4;
        //Column exp = expr("mod(partitionColHash,+"+seed+")");
        dsEmp= dsEmp.withColumn("partitionColHash",monotonically_increasing_id() )
                    .withColumn("partitionColIndex",expr("mod(partitionColHash,+"+seed+")"))
                    .repartition(seed,col("partitionColIndex"))
                    .withColumn("date_col",current_date().cast(DataTypes.DateType))
                    .withColumn("num_col",lit("0.0").cast(DataTypes.createDecimalType()))
                    .withColumn("str_col",lit("NA").cast(DataTypes.StringType));

        System.out.println("After repartitioning show :: "+dsEmp.rdd().getNumPartitions());
        dsEmp.show();
       //dsEmp.write().partitionBy("emp_deptno").save("./data/emp_partitioned");
        System.out.println("from file");
        Dataset dsp = spark.read().parquet("./data/emp_partitioned");


        Dataset<Row> dsEmpSave = null;
        for(int i =0;i<4;i++){
           System.out.println("i value :: "+i);
          // dsEmp.where(col("partitionColIndex").equalTo(lit(i))).show();
            dsEmpSave = dsEmp.where(col("partitionColIndex").equalTo(lit(i)));
            dsEmpSave.show();
         //   dsEmpSave.write().format("jdbc").options(optionWrite).mode(SaveMode.Append).save();
           // dsEmpSave.write().parquet("./store/"+i);
        }

        Dataset<Row> dsStore = spark.read().parquet("./store/*");
        System.out.println("dsStore :: "+dsStore.rdd().getNumPartitions());
        dsStore.show();

        /*
       the mode is ‘overwrite‘.
       It will truncate the table and insert records if table present. Otherwise, it will create a new table.

        */




        spark.stop();
    }
/*

    private static void saveToDB(Dataset ds,String tableName){
        ds.write().format("jdbc").mode(SaveMode.Overwrite).
                option("url", "jdbc:oracle:thin:@localhost:1521:AnekDB").
                option("driver", "oracle.jdbc.driver.OracleDriver").
                option("continueBatchOnError","true").
                option("user", "finance").
                option("password", "finance").
                option("dbtable",tableName).
                option("truncate","true").
                option("numPartitions","4").
                option.put("partitionColumn","EMP_DEPTNO").
                option.put("lowerBound","0").
                option.put("upperBound","10000").
                        save() ;



               */
/* option("partitionColumn","JOIN_DATE").
                option("lowerBound","2020-06-01").
                option("upperBound","2021-02-01").
                option("numPartitions","3").
                option("dateFormat", "yyyy-MM-dd HH24:MI:SS").
                option("sessionInitStatement",
                        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-MM-dd HH24:MI:SS' ").*//*

    }

*/

   

}
