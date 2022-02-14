package udemy_practice.scala_fundamentals.module2

/**
 * created by ANEK on Wednesday 10/21/2020 at 9:44 AM 
 */
object val_var_ex {

  def main(args: Array[String]): Unit = {
    // here ,again you are defining  val x  ,so you have to define in new scope or else CTE
    val x  = 10
    println(x)

    {
      val x = 20
      println(x)

    }



  }

}
