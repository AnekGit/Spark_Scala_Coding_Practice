package spark_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object remove_duplicates {
  case class Person (name :String,age : Int,location :String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("remove_dup").getOrCreate()

    import spark.implicits._

    val data = Seq(
        Person("Rajesh",21,"London"),
        Person("Suresh",28,"California"),
        Person("Sam",26,"Delhi"),
        Person("Rajesh",21,"Gurgaon"),
        Person("Manish",29,"Bangalore"),
        Person("Anek",28,"Dhanbad")
    )  /* add .toDS when working with spark.sql*/

    // remove duplicates

  /*  val spec = Window.partitionBy("name").orderBy("location")
    
     data.withColumn("flag",dense_rank().over(spec))
       .filter($"flag" === 1 )
       .drop("flag").show()
*/

      // with core programming

     val res= data.map( r => ( (r.name.toString,r.age),r.location)  )
         .groupBy(_._1)
         .map(_._2.head)
      val rd  = res.map(r => Person(r._1._1,r._1._2,r._2) ).toList

    print(rd)




  }

}
