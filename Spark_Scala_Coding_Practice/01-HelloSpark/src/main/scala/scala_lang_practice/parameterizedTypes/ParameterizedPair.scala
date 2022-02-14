package scala_lang_practice.parameterizedTypes

import scala_lang_practice.parameterizedTypes.ParameterizedPairAny.PairAny


object ParameterizedPair {

  case class Pair[T,U](first : T,second : U){

    def switch : Pair[U,T] = this.copy(first=second, second=first)
    override def toString: String = s"($first,$second)"

  }

  def main(args: Array[String]): Unit = {

    val intStringPair: Pair[Int, String] = Pair(1,"Anek")
    println(s"intStringPair :: ${intStringPair}")

    val switched: Pair[String, Int] = intStringPair.switch
    println(s"switched :: $switched")

    val first: String = intStringPair.switch.first
    println(s"first :: $first")

    val second : Int = intStringPair.switch.second
    println(s"second :: $second")
  }

}
