package spark_practice.interview_questions;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SizeEstimator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import spark_practice.TestWrapper;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

public class Create_Datasets_Methods {
    public static class PersonBean implements Serializable {
        int id;
        String name;
        String dob;

        public PersonBean(){}
        public PersonBean(int id, String name, String dob ){
            this.id = id;
            this.name = name;
            this.dob = dob;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDob() {
            return dob;
        }

        public void setDob(String dob) {
            this.dob = dob;
        }
    }
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                 .set("master","local[2]")
                .set("spark.sql.shuffle.partitions","2");
        SparkSession spark = SparkSession.builder().appName("EncodersEx")
                                          .master("local[2]")
                                          .config(conf)
                                          //.enableHiveSupport()
                                          .getOrCreate() ;


        System.out.println(spark);



        List<List<Integer>> arr = new ArrayList<>();

        Integer[][] a = new Integer[arr.size()][];
        for (int i=0; i<arr.size(); i++) {
            List<Integer> row = arr.get(i);
           // row.toArray(new Integer[row.size()])   ;
            a[i] = row.toArray(new Integer[row.size()]);
        }
        System.out.println(Arrays.deepToString(a));



        System.out.println("***************  METHOD 1 ::    ****************");
        List<Row> rowList = Arrays.asList(
                RowFactory.create("anek",10000.0)
        );
        StructType schemaM = new StructType()
                .add("name",DataTypes.StringType,Boolean.TRUE)
                .add("sal",DataTypes.DoubleType,Boolean.TRUE);

        Seq<Row> rowSeq = JavaConverters.iterableAsScalaIterableConverter(rowList).asScala().toSeq();
        ClassTag<Row> tag = scala.reflect.ClassTag$.MODULE$.apply(Row.class);

        final RDD<Row> rdd = spark.sparkContext().parallelize(rowSeq, 2, tag);

        Dataset<Row> dsM = spark.createDataFrame(rdd,schemaM);
        dsM.show();

        System.out.println(rdd.collect().toString());
        System.out.println("***************  METHOD 1 ::    ****************");

        Dataset<Integer> ds = spark.createDataset(Arrays.asList(1,2), Encoders.INT());
        ds.printSchema();


        List<Row> listOfRows =  Arrays.asList(
                RowFactory.create("1","ANEK","2020-09-28"),
                RowFactory.create("2","Manish","2014-12-29"),
                RowFactory.create("3","Aaryan","2014-12-02")
        );


        StructType structType = new StructType()
                .add("ids", DataTypes.StringType)
                .add("name",DataTypes.StringType)
                .add("joining_date",DataTypes.StringType);

        ExpressionEncoder encoder = RowEncoder.apply(structType);

        //Dataset dataset = spark.createDataset(Arrays.asList("1","2","3"),structType);

        Dataset<Row> dsRows = spark.createDataFrame(listOfRows,structType);
        
        dsRows.printSchema();
        dsRows.show();

        System.out.println("================================================================");
        StructType structType2 = new StructType()
                .add("ids", DataTypes.StringType)
                .add("name",DataTypes.StringType)
                .add("joining_date",DataTypes.DateType);

        List<Row> listOfRows2 =  Arrays.asList(
                RowFactory.create("1","ANEK", Date.valueOf("2020-09-28")),
                RowFactory.create("2","Manish",Date.valueOf("2014-12-29")),
                RowFactory.create("3","Aaryan",Date.valueOf("2014-12-02"))
        );

        Dataset<Row> dsRows2 = spark.createDataFrame(listOfRows2,structType2);

        dsRows2.printSchema();
        dsRows2.show();

        System.out.println("SizeEstimator estimate  :: "+SizeEstimator.estimate(dsRows2));

        //dsRows2.rdd().map(e -> e.getByte("UTF-8")).

        System.out.println("=================================================================");
              

        System.out.println("After conversion  \n");

        dsRows = dsRows.withColumn("joining_date",to_date( col("joining_date"),"yyyy-MM-dd") );
        dsRows.printSchema();
        dsRows.show();

        List<PersonBean> personBeanList = Arrays.asList(
                new PersonBean(10,"Anek","2020-09-02"),
                new PersonBean(20,"Mom","2019-11-12")

        );
        Dataset<PersonBean> dsPerson = spark.createDataset(personBeanList, Encoders.bean(PersonBean.class));
        dsPerson.printSchema();
        dsPerson.show();
        Dataset dsPerson1 = dsPerson.withColumn("dob",to_date(col("dob"),"yyyy-MM-dd"));
        dsPerson1.printSchema();
        dsPerson1.filter(" id > 10").show();
      /*  ds.show();
        ds.map((MapFunction) value -> {
                     return (Integer)value + 1;
                },Encoders.INT()
        ).show();*/

        // dsPerson.repartition(1).write().partitionBy("dob").save("./store/123");

        ExpressionEncoder<TestWrapper> en = (ExpressionEncoder<TestWrapper>) Encoders.bean(TestWrapper.class);

        TestWrapper tw = new TestWrapper();
        InternalRow row = en.toRow(tw);
        System.out.println("num fields : : "+row.numFields());
       // System.out.println("row.getString :: "+row.getString(0));
     /*   System.out.println("row.getString :: "+row.getString(1));
        System.out.println("row.getMap :: "+row.getMap(0));*/
        //System.out.println("row.getList :: "+row.get(0,DataTypes.ma));







        spark.stop();
    }

}
