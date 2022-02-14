package udemy_practice.scala_fundamentals.module2
import scala.collection._
object mutable_immutable {
  def main(args: Array[String]): Unit = {

    val s1 = mutable.Set(1,2,3)


    println(s"s1 - 2 : ${s1 - 2}" )
    println(s"s1 : ${s1}")

    println(s"s1 -= 2 : ${s1 -= 2 }")
    /*
     * here rewriting rule takes place  :=> Mutate the list since val cant be reassigned
    */
    println(s"s1 : ${s1}")
    println("*********************************************")

    var s2 = immutable.Set(1,2,3)

    println(s"s2 - 2 :  ${s2-2} ")
    println(s"s2 is : ${s2} ")

    println(s"s2 -= ${s2 = s2- 1}")
    println(s"s2 is : ${s2} ")



  }


}
