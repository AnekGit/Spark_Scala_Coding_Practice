package spark_practice;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JavaParallelize {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Spark_3_Software");
        SparkSession spark;
        spark = SparkSession.builder()
                .master("local[3]")
                .config("spark.sql.shuffle.partitions", 2)
                .appName("SparkLoaderTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        List<Integer> list = Arrays.asList(1,2);
        List<Row> listOfRow =  list.stream().map(e  ->  RowFactory.create(e) ).collect(Collectors.toList());
        Seq<Integer> seq = JavaConverters.asScalaBuffer(list).toSeq();

        ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
        RDD<Integer> rdd = spark.sparkContext().parallelize(seq,2,tag);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Integer> intJavaRDD = jsc.parallelize(list);
        System.out.println(intJavaRDD.collect());

        StructType schema = DataTypes.createStructType(
                             Arrays.asList(
                                 new StructField("id",DataTypes.IntegerType,false,Metadata.empty())

                                 )
        );
       List<String> nameList = Arrays.asList("Anek,Neha,Pramila");
       JavaRDD<String> nameListJavaRDD = jsc.parallelize(nameList);
        spark.createDataFrame(listOfRow,schema).show();
        spark.createDataFrame(jsc.parallelize(listOfRow),schema).show();

        System.out.println("********** DataSet *****************");
        Dataset<Integer> dsInteger = spark.createDataset(list,Encoders.INT());
        dsInteger.show();

       spark.createDataset(seq,Encoders.INT()).show();
       spark.createDataset(rdd,Encoders.INT()).show();
       spark.createDataset(nameList, Encoders.STRING()).show();
       spark.createDataset(nameListJavaRDD.rdd(),Encoders.STRING()).show();
       spark.createDataset(intJavaRDD.rdd(),Encoders.INT()).show();
       System.out.println("************* done *****************");

       List<Person> personList = list.stream().map( e -> new Person(e) ).collect(Collectors.toList());
       personList.forEach(System.out::println);

       JavaRDD<Person> personJavaRDD = jsc.parallelize(personList);
       RDD<Person> personRDD = personJavaRDD.rdd();
       System.out.println("personJavaRDD collect  :: \n "+personJavaRDD.collect());
       System.out.println("personRDD    collect  :: \n "+personRDD.collect());
        /*
        * .createDataset() accepts RDD<T> not JavaRDD<T>.
        * JavaRDD is a wrapper around RDD inorder to make calls from java code easier.
        * It contains RDD internally and that can be accessed using .rdd(). The following can create a Dataset:
        *       Dataset<Person> personDS =  sqlContext.createDataset(personRDD.rdd(), Encoders.bean(Person.class));
        * */
       spark.sqlContext().createDataset(personRDD,Encoders.bean(Person.class)).show();

       spark.createDataset(personList,Encoders.bean(Person.class)).show();

        List<Person> listOfPersons = Arrays.asList(
                new Person(901),new Person(1001));

        JavaRDD<Person> personJRDD = jsc.parallelize(listOfPersons,2);
        Dataset<Person> dsPerson = spark.sqlContext().createDataset(personJRDD.rdd(),Encoders.bean(Person.class));
           dsPerson.printSchema();
           dsPerson.show();

    }
}
