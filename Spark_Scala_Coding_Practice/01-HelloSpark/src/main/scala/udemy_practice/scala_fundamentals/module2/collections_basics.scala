package udemy_practice.scala_fundamentals.module2
object collections_basics {
  def main(args: Array[String]): Unit = {

    // Nil is an EMPTY LIST
    // Nil is right associative means read the Nil operations from right to left
    // :: => called cons
    // :: prepends NOT append


   val list = Nil
   println(list)

   val list1 = Nil.::(3).::(2).::(1)
   println(list1)

   //short cut to write above expression
   val list2 = 1 :: 2 :: 3 :: Nil
   println(list2)

   val xs1 = List(1,2,3)
   val xs2 = List(4,5,6)

    println("cons test 1 "+ xs1.::(xs2) )     // NOT append
    println( xs1 ::: xs2  )          // Append 

    val xs3 = 1
    
    println( xs3 :: xs2  )


   // Lists in scala implements LinkedList underneath :=> useful while working with head of List

   val l1 : Seq[Int] = List(1,2,3)
   val a1 : Seq[Int] = Array(1,2,3)
   
   println(s"seq list : ${l1}")
   println(s"seq array : ${a1}")


   /*
   *    Here you can pass List as seq to the below method
   *    BUT when you pass Set as seq then CTE
   *
   */
   def squareOfAll(in : Seq[Int]) : Seq[Int] = in.map( e => e * e )

   println(s"square of list seq  : ${squareOfAll(l1)} "  )
   println(s"square of array seq  : ${squareOfAll(a1)} " )


   /*
   *  Sets in scala implements HashSet underneath .
   *
   */
   val s1 = Set(5,6,7)
  // println(s"square of all  : ${squareOfAll(s1)} "  )


   /*
   *  Vectors in scala is not same as old Java vectors initially developed
   *  It is more robust in scala 
   */

   val v1 = Vector(1,2,3)
   println(s"Vector is ${v1}")

   //v1: scala.collection.immutable.Vector[Int] = Vector(1, 2, 3)










  }




}
