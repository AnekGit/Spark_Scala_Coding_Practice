package udemy_practice.scala_fundamentals.module2
import scala.io._

object read_file {
  def main(args: Array[String]): Unit = {
    val path = "F:\\\\Spark_workspace\\\\SparkProgrammingInScala-master\\\\01-HelloSpark\\\\src\\\\main\\\\scala\\\\udemy_practice\\\\scala_fundamentals\\\\module2\\\\test_file.txt"


    for(lines <- Source.fromFile(path).getLines()){
      println(s"${lines}")
    }




  }

}
