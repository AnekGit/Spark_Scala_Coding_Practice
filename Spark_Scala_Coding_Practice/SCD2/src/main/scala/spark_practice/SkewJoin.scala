package spark_practice

package spark_practice
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoders, SparkSession}


object SkewJoin {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args :Array[String]) :Unit = {

    //***************      import spark.implicits._   **************

    //****************      import spark.implicits._   ***************
/*
REMOVE DUPLICATE
       delete from customer_expense where rowid not in (
      select max(c.rowid) from customer_expense c group by c.cid  );


      */
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","3").master("local[3]").appName("SCD2")
   .getOrCreate()


    val data = Seq(
      ("2020-07-06 01:20:21.739","u1"),
      ("2020-07-07 01:55:21.739","u1"),
      ("2020-07-06 01:35:21.739","u1") ,
      ("2020-07-06 01:50:21.739","u1"),
      ("2020-07-06 01:55:21.739","u1"),
      ("2020-07-06 01:55:21.739","u2"),
      ("2020-07-06 01:25:21.739","u2"),

    )

    import spark.implicits._
    val right = spark.sparkContext.parallelize(Seq("u2","u1"),2)
    import spark.implicits._

    /* split() use case */
    println("****************  2 ways :  split() use case ******************")
    val d = Seq("0,1,2")
    val rd =spark.sparkContext.parallelize(d).toDF("exp")
         rd.show
    var rdDF1 = rd.withColumn("explode_column",explode( split(col("exp"),",") )  )

    var rdDF2 = rd.withColumn("explode_column",explode( array(Array(1,2,3).map( lit(_) ):_* )  )   )
    println("***** rdf1")
    rdDF1.show
    rdDF2.show

    println("******************                      *****************************")
    var rightDF = right.toDF("user_right")
    rightDF.show

    /*  SKEW JOIN CODE  */
    val rdd = spark.sparkContext.parallelize(data,2)

    var leftDF  = rdd.toDF("time","user")

    leftDF.groupBy("user").agg(count("*").as("count_value")).show()

     println(array( (0 to 2).map( lit(_) ) : _ *) )


    leftDF = leftDF
      .withColumn("user",concat(col("user"),
      lit("_"),
      lit( floor(rand(1234)*3) )   ))

    println("**********************  leftDF.show")
    leftDF.show

    // array of lit that returns Column type
    // Consider lit(_) as values of a Column
    // i.e. array(0,1,2) = array( lit(0):Column ,lit(1):Column , lit(2):Column  )where 0,1,2 are of Column Type see
    // array def
    rightDF = rightDF
      .withColumn("explode_col", explode(  array(  (0 to 2).map( lit(_) ): _ * )   ) )

    println("******************************    check")


    rightDF = rightDF
      .withColumn("user_col",
        concat(
          col("user_right"),lit("_"),col("explode_col") )
      )

    rightDF.show

    val join = leftDF.join(rightDF,col("user") <=> col("user_col"))
    join.show
            join.queryExecution.sparkPlan
    join.explain()
    join.drop("user_col","explode_col")
      .withColumn("user_id",split( col("user"),"_"  )(0)   )
      .selectExpr("time","user_id").show

  

    print(spark)

    spark.stop

  }

}
