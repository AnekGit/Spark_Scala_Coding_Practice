package udemy_practice.scala_fundamentals.module3

object for_ex {
  def main(args: Array[String]): Unit = {


    for(i <- 1 to 5 ; j <- 1 to 3 ){

      println(i , j)

    }


    val f = for{
      i <- 1 to 5
      j <- 2 to 4
    }  yield (i,j)

    println(f)




  }
}
