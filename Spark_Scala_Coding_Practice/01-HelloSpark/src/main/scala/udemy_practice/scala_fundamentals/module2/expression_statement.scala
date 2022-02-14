package udemy_practice.scala_fundamentals.module2

/**
 * created by ANEK on Sunday 10/18/2020 at 9:46 AM 
 */
object expression_statement {
  def main(args: Array[String]): Unit = {
    val x : Int = 10
    val y : Int = 20

    // expression : it will always return a value
    val min = if (x > y) x else y
    println(s"expression returning  : ${min}")

    //statement : it has got side effects
    val r = if (x > y) println(s"x is : ${x}") else println(s"y is : ${y}")

    println(s"statement returning :  ${r}")

    val x1 = 5
    println(s"x1 is ${x1} ")
    println(x1)


    var x11 = 15
    println(s" var x11 is : ${x11}")

    def m12(x :Int ,y :Int ) = {
      x+y

    }
    def m1(x :Int ,y :Int ) = {
        val z =  x+y
        // you have to give here return  " z "  or else you will get Unit as output
    }

    println(m12(4,4))
    println( m1(3,3) )

  }

}
