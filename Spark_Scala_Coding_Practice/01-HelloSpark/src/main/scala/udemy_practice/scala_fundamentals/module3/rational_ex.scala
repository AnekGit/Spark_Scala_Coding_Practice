package udemy_practice.scala_fundamentals.module3

object rational_ex {
  def main(args: Array[String]): Unit = {

    val first = new Rational(1,2)
    val second = new Rational(2,3)
    val fifth = new Rational(1)
    /*
     println(first,second)

     println(s"calculating min :   ${first.min(second)}")

    */
   // println(s"fifth : ${fifth}")

    val add = first.+(fifth)
    println(s"adding :  ${add}")

    println()
  }

}

class Rational (val n :Int , val d :Int){    // Primary constructor

  require(d != 0 ,"please input valid number")
  println(s" calling constructor : :  num : ${n} & den : ${d}")

  /*  Auxiliary constructor   */                         
  def this(i : Int) = {
    this(i,1)
    println(s"auxiliary called ")
   
  }

  override def toString : String = s"Rational(${n}/${d})"

  def min(other : Rational) : Rational = {

    if (other.n < this.n) other else this

  }
  def +(other: Rational) : Rational = {
    new Rational(
      this.n * other.d + other.n * this.d ,this.d * other.d

    )
  }




}