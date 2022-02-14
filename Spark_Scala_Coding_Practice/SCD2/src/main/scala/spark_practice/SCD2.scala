package spark_practice
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoders, SparkSession}

object SCD2 extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val prefix:String = "input_"
  val REMOVE:String ="remove"
  val INSERT:String = "insert"
  val DUPLICATE:String = "duplicate"
  
  def main(args : Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("SCD2").getOrCreate()

    val current_df = spark.read
        .option("header","true")
        .option("inferSchema","true")
        .format("csv")
        .load("data/persons.csv")

    //current_df.printSchema()
    //current_df.show()

    val new_df = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .format("csv")
      .load("data/new_persons.csv")

    //new_df.show

    val naturalCols = Array("ID","Name","Tax")

    val originalDf_prev = current_df.withColumnRenamed("Tax_Percentage(%)","Tax")
    val latestDf_prev = new_df.withColumnRenamed("Tax_Percentage(%)","Tax")

    val originalDf = current_df.withColumnRenamed("Tax_Percentage(%)","Tax").selectExpr(naturalCols:_*)
    originalDf.show()

    val latestDf = new_df.withColumnRenamed("Tax_Percentage(%)","Tax").selectExpr(naturalCols:_*)
    latestDf.show()

    // column expression for left
    var left = naturalCols.map(e => col("input_"+e)).toList
    val leftDf = latestDf
      .withColumnRenamed("ID",prefix+"ID")
      .withColumnRenamed("Name",prefix+"Name")
      .withColumnRenamed("Tax",prefix+"Tax")


    val leftColExp = col(prefix+"ID").isNull
      .and(
        col(prefix+"Name").isNull
      )
      .and(
        col(prefix+"Tax").isNull
      )
     val rightColExp = col("ID").isNull
       .and(
         col("Name").isNull
       )
       .and(
         col("Tax").isNull
       )
    val joinExp = col(prefix+"ID").eqNullSafe(col("ID"))
      .and(
        col(prefix+"Name").eqNullSafe(col("Name"))
      )
      .and(
        col(prefix+"Tax").eqNullSafe(col("Tax"))

      )



    var join = leftDf.join(originalDf,joinExp,"outer")
                     .withColumn("record_type",
                                                    when(leftColExp,REMOVE).otherwise(
                                                      when(rightColExp,INSERT).otherwise(DUPLICATE)
                                                    )
                           )
    println(join.queryExecution.sparkPlan)
    join.show()
    //join = join.drop()
    val toBeRetained = join.where(col("record_type").equalTo(DUPLICATE)).selectExpr(naturalCols:_*)
    println("to be retained")
    toBeRetained.show()

    val toBeInserted = join.where(col("record_type").equalTo(INSERT))
                           .selectExpr(prefix+"ID",prefix+"Name", prefix+"Tax")
      .withColumnRenamed(prefix+"ID","ID")
      .withColumnRenamed(prefix+"Name","Name")
      .withColumnRenamed(prefix+"Tax","Tax")

    println("to be inserted")
    toBeInserted.show()

    val toBeRemoved = join.where(col("record_type").equalTo(REMOVE)).selectExpr(naturalCols:_*)
    println("to be removed")
    toBeRemoved.show()


    val dup =
      originalDf_prev.join(toBeRetained.withColumnRenamed("ID",prefix+"ID")
       .withColumnRenamed("Name",prefix+"Name")
       .withColumnRenamed("Tax",prefix+"Tax"),joinExp)
       .drop(prefix+"ID",prefix+"Name",prefix+"Tax")
    
    val rem =
      originalDf_prev.join(toBeRemoved.withColumnRenamed("ID",prefix+"ID")
        .withColumnRenamed("Name",prefix+"Name")
        .withColumnRenamed("Tax",prefix+"Tax"),joinExp)
        .drop(prefix+"ID",prefix+"Name",prefix+"Tax")
        .withColumn("Effective_Upto",current_timestamp())

    val ins = toBeInserted.withColumn("Effective_From",current_timestamp())
      .withColumn("Effective_Upto",lit("null"))

    val finalDF = dup.unionAll(rem).unionAll(ins).orderBy("Name")
    finalDF.show(truncate = false)
   


    /*
      1st  RETAIN
      inner join                       1  emp1  10   date  null

      2nd  INSERT
      new.join(current ,"left_anti")   2  emp1  17   newdate  null
                                       4	emp1  15   newdate  null


     3rd  DELETE
     current.join(new ,"left_anti")	 2   empl  15   date  Enddate
                                     3   empl  15   date  Enddate
    */

   /* // 1 . calculate record hash
    val dropped_cols_current_df = current_df.drop("Effective_From","Effective_Upto")

    val lookup_df = current_df
      .withColumn("record_hash",
                             sha1(  concat(col("ID"),col("Name"),col("Tax_Percentage(%)"))  )
      ).show()

    val latest_df =  new_df
      .withColumn("record_hash_new",
         sha1(  concat(col("ID"),col("Name"),col("Tax_Percentage(%)"))  )
       ).show()
*/
    import spark.implicits._
 /*   lookup_df.join(latest_df,
                   col("record_hash").equals(col("record_hash_new")),
                  "inner"
               )*/
    // prepare the narutal columns







    val reviewFlag_data = Seq(
                  (1,"Jharkhand",Array("Dhanbad","Ranchi")) ,
                  (2,"Bihar",Array("Katihar","Patna"))
                  )
    val rdd = spark.sparkContext.parallelize(reviewFlag_data,2)
    val reviewFlag = rdd.toDF("id","state","cities")

    println("********* reviewFlag ******************")

    reviewFlag.printSchema
    reviewFlag.show()
    val reviewFlag_explode = reviewFlag.withColumn("cities_explode",explode(  col("cities") ))

    val list = List("Dhanbad","Patna")
    val rule =  spark.createDataset(list)
    rule.show

    /*
    * Here is what it does:

Returns same result with EQUAL(=) operator for non-null operands
however:

it returns TRUE if both are NULL

it returns FALSE if one of them is NULL
    * */

    val joinedRow = reviewFlag_explode.join(rule,
                        col("cities_explode") <=> (col("value")),
                        "left_outer"
                        )
      .withColumnRenamed("cities_explode","city")
      .withColumn("value_error"
                                  ,when(col("value").isNotNull,"1").otherwise("0")
                 )
      .select($"id",$"state",$"city",$"value",$"value_error").cache()
    joinedRow.withColumn("rank",dense_rank().over(Window.partitionBy("state").orderBy(col("value_error"))) )
      .show()

  /* val data2 = Seq(


                    (1,"JH","DHN,BOK") ,
                    (2,"BHR","KAT,PAT")
                   )
    val rdd2 = spark.sparkContext.parallelize(data,2)
    val ds2 = rdd2.toDF("id","state","cities")
    ds2.withColumn("cities",split(col("cities"),",")).show

           ds2.show()*/










    spark.stop()

  }

}
