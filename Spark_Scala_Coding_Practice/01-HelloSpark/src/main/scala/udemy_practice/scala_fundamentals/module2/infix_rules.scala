package udemy_practice.scala_fundamentals.module2

object infix_rules {
  def main(args: Array[String]): Unit = {

    val s = "anek"
    println( s charAt 1  )

    val i = 1
    println( 1 + 2  )
    println( 1.+(3) )

    val list = List(1,2,3)
    println(  list.map(_*2)  )
    println(  list map {_*3}  )

    // println "hello"   => will not compile since infix requires instance to operate on

    System.out println "hello"
    // System.out is an instance on which this println method is called


    val args = Array("Ram","Krishana")

    println( args apply 0 )
    println( args(1)  )
                
    println(args.length)

    println( args.update(1,"Ram_updated")  )

    for(i <- args){

      println(i)

    }










  }

}
