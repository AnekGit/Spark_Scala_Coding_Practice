package scala_lang_practice

object match_expressions {
  def main(args: Array[String]): Unit = {
    val seq : Seq[String]= Seq("A","B","C","D")
     //val range = Range(2)

    val s = "neha"

    val out =  s match {
        case "anek" => s"Hi I am ${s}"
        case "neha" =>  s"Hi I am ${s}"
        case _ => s"Hi I am ${s}"
     }
    case class Person(name :String ,age : String , courses :List[String])
    val person = Person("vicky","31",List("scala","spark"))

    val pout = person match {
      case Person(n,a,s) => s"Person :: $n , $a ,${s} "
      case _  => s"Person no values"

    }

    // trick 1

    val countingList = List(1,2,3,42)
    val mustHaveThree = countingList match {
      case List(_, s, 3, somethingElse) => s"A-HA! I've got a list ${s} with 3 as third element," +
        s" I found ${somethingElse} after"
    }

    // trick 2
    val emptyList = List()
    val trick2 = countingList match {
      case Nil => s"List is empty "
      case List(1,2,_*) => s"except 1 and 2  list is :: ${}"
    }
    println(s"trick2 :: ${trick2}")

    //trick 3 : Haskell like

    val trick3 = countingList match {
      case 1 :: tail => s"list starsWith 1 with tail as ${tail}"
    }
    println(s"trick3 : ${trick3}")

    val trick4 = countingList match {
      case head :: tail => s"list starsWith head as ${head} with tail as ${tail}"
    }
    println(s"trick4 : ${trick4}")

    val trick5 = countingList match {
      case List(_*) :+ 42 => s"list is long"
    }
    println(s"trick4 : ${trick5}")






    




    println(mustHaveThree)




    
    println(pout)
      
    println(out)



    // scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/

  }

}
