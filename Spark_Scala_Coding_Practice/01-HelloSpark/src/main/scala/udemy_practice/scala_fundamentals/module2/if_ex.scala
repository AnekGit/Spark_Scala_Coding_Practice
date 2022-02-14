package udemy_practice.scala_fundamentals.module2

/**
 * created by ANEK on Wednesday 10/21/2020 at 9:44 AM 
 */
object if_ex {

  def main(args: Array[String]): Unit = {

    var newName = if (args.length > 0) args(0) else "default.txt"
    println(newName)

    if(args.length > 0 ) {
      newName = "d1.txt";
    }else{
      newName = "d2.txt" ;
    }


    println(newName)

  }


}
