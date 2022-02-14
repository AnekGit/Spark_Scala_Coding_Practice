package udemy_practice.scala_fundamentals.module3
import scala.language.implicitConversions
import scala.annotation.tailrec

object try_ex {
  def main(args: Array[String]): Unit = {
  //args.update(0,"anek")

    var tt = try{
    args(0)
  }catch {
    case _ : NoSuchElementException => "default.txt"
    case _ : ArrayIndexOutOfBoundsException => "Array Out Of Bound"
  } finally {
    println(s"in finally block ")
    
  }

    val tail_re = new tail_recursion
    println(tail_re.times(5))

   println(tt)


  }

}

class tail_recursion{

  @tailrec
  final def times(n: Int, cur :Int = 0 ) : Unit = {

    if (cur < n ) {
      println(s"hello : ${cur}")
      times(n,cur+1)

    }




  }

}


