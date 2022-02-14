package spark_practice.interview_questions

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object reduce_fold_scan {
  def main(args: Array[String]): Unit = {
    val seq : Seq[String]= Seq("A","B","C","D")

    // 1 .  reduce
   println( "reduce:: "+seq.reduce( (a:String,b:String) => {println("a = "+a+" ,b = "+b); a.concat(b)} ) )

    // 2 .  reduceLeft
   println( "reduceLeft :: "+seq.reduceLeft( (a:String,b:String) => {println("a = "+a+" ,b = "+b); a.concat(b)} ) )

    // 3 .  reduceRight
    println( "reduceRight :: "+seq.reduceRight( (a:String,b:String) => {println("a = "+a+" ,b = "+b); a.concat(b)} ) )

    // 4 .  scan
    println( "reduceRight :: "+seq.scan("M")( (a:String,b:String) => a.concat(b) ) )

    // 1 .  fold
    println( "fold:: "+seq.fold("z")( (a:String,b:String) => {println("a = "+a+" ,b = "+b); a.concat(b)} ) )

    // 2 .  foldLeft
    println( "foldLeft :: "+seq.foldLeft("z")( (a:String,b:String) => {println("a = "+a+" ,b = "+b); a.concat(b)} ) )

    // 3 .  foldRight
    println( "foldRight :: "+seq.foldRight("z")( (a:String,b:String) => {println("a = "+a+" ,b = "+b); a.concat(b)} ) )










    // scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/

  }

}
