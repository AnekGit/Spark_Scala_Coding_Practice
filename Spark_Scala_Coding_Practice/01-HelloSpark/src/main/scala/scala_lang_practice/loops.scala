package scala_lang_practice

object loops {
  def main(args: Array[String]): Unit = {
    val seq : Seq[String]= Seq("A","B","C","D")
     //val range = Range(2)

    for(i <- 1 to 5){
      println(s"using i => ${i}")
    }
    for(i <- 1 until 6){
      println(s"using until i => ${i}")
    }
    for(i <- 1 to 3; j <- 1 to 3){
      println(s"using generator in var (i,j)  => ${i},${j}")
    }
    for(i <- 1 to 10 if i > 7) {
      println(s"filtered value of i is ${i} ")
    }
    // for loop as an expression ::

    val forExp = for{  i <- 1 to 10 if i < 4
    } yield i

    println(forExp.toList)


    


    // scala.io.StdIn.readLine()
    // to hold your job and see the spark UI   http://desktop-sac6nj1:4040/

  }

}
