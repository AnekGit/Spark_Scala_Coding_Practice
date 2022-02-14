package udemy_practice.scala_fundamentals.module4

import org.apache.parquet.format.LogicalTypes.TimeUnits

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
object for_ex {

  def main(args: Array[String]): Unit = {

    val f1 = Future(1.0)
    val f2 = Future(2.0)
    val f3 = Future(3.0)

    val result = for {
     v1 <- f1
     v2<-f2
     v3<-f3
    } yield v1+v2+v3

    println("before success")
       result onComplete  {
      case result => println(s"total = $result")
    }

  }

}
