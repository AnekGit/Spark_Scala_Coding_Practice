package scala_lang_practice.parameterizedTypes

object ParameterizedPairAny {
  case class PairAny(first: Any,second : Any ){
    
    def switch : PairAny = this.copy(first = second ,second = first)

    override def toString: String = s"($first,$second)"

  }
  def main(args: Array[String]): Unit = {

    val intStringPair  = PairAny(1,"Anek")
    println(s"intStringPair :: ${intStringPair}")

    val switched = intStringPair.switch
    println(s"switched :: $switched")

    val first = intStringPair.switch.first
    println(s"first :: $first")

    //  val second : Int = intStringPair.switch.second        ==> CTE
    val second  = intStringPair.switch.second               //==> fine because we are not referring type
    println(s"second :: $second")

  }
}
