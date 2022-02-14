package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DataTypes}

object collection_set_join_Ex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("caching").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions","2");
    import spark.implicits._
    val ds1 =  Seq(
      (111,201),
      (111,2011),
      (111,2011),
      (112,202),
      (113,2055),
      (113,2055)

    ).toDF("pair","acct_number_1")

    var s: String = "111"
    println("111 +++++++++++++++++++++++++++++++++++++++++++")
    ds1.withColumn("item_flag",
      when( col("pair").gt(s),1)  

    ).show()


    println("===========================================================")
    val ds12 =  Seq(
      (111,201),
      (111,2011),
      (111,2011),
      (112,202),
      (113,2055),
      (113,2055)

    ).toDF("pair","acct_number_1")
      .withColumn("acct_number_1",col("acct_number_1").cast(DataTypes.IntegerType))
    val LISTAGG = udf((x : Seq[String]) => x.mkString("->"))
    ds12.groupBy($"pair")
      .agg(
       LISTAGG( collect_list($"acct_number_1") )     // here return type of collect_list is of Column type
      ).show



    println("===========================================================")

    val ds2 =  Seq(
      (1,2011),
      (1,2012),
      (1,2011),
     
      (3,2055),
      (3,2056)

    ).toDF("trans_ref","acct_number_2")
    val dataset =  Seq(
      (1,2011,"V01"),
      (1,2012,"V02"),
      (1,2011,"V01"),

      (3,2055,"V01"),
      (3,2056,"V01")

    ).toDF("trans_ref","acct_number_2","acc_branch")

    val windowSpec = Window.partitionBy($"trans_ref")
    var dataset_grp = dataset.withColumn("account_set",collect_set($"acct_number_2").over(windowSpec))
    var dataset_branch = dataset_grp.withColumn("branch_set",collect_set($"acc_branch").over(windowSpec))
    println("printing dataset_grp")
    dataset_grp.show()

    println("branch present dataset_branch")
    dataset_branch.withColumn(
      "branch_present",
                                  array_contains($"branch_set","V01")
                                    .or(
                                      array_contains($"branch_set","V02")
                                    )
    ).filter($"branch_present".equalTo("true")).show

    
    var ds1_g = ds1.groupBy("pair").agg( collect_list($"acct_number_1").as("coll_1") )
   // ds1_g.show
    //ds1_g.withColumn("coll_1_upack",explode($"coll_1")).show


    var ds2_g = ds2.groupBy("trans_ref").agg( collect_list($"acct_number_2").as("coll_2") )
   // ds2_g.show
    //ds2_g.withColumn("coll2_upack",explode($"coll_2")).show
    
    val ds3 = Seq("2055").toDF("c3")
    //ds2_g.

      val ds3_g3 = Seq("3").toDF("trans_ref")
     ds1_g.as("left")
                    .join(ds2_g.as("right"),
                    $"coll_1" === $"coll_2",
                    "inner")
                   .join(ds3_g3.as("right1"),
                     $"right.trans_ref" === $"right1.trans_ref",
                     "inner")
        .select("left.*","right.trans_ref","right1.trans_ref") .show




    //final1.withColumn( "coll2_unpack",explode($"coll2") ).show
    

    spark.stop

  }

}
