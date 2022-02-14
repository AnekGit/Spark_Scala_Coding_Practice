package scala_lang_practice.parameterizedTypes

/**
 * created by ANEK on Saturday 8/28/2021 at 9:35 PM 
 */

object ParameterizedMethods {

  //def add(a:Int,b:Int) : Int = a + b
  def operationP[T](a : T,b :T ,f : (T,T) => T) :T = f(a,b)

  def main(args: Array[String]): Unit = {

    val addition = operationP(1,2,(a:Int, b:Int) => a + b)
    val addition2 = operationP(2.0f,2.0f,(a:Float, b:Float) => a + b)
    val subtraction = operationP(2.0f,1.0f,(a:Float, b:Float) => a - b)
    println(s"addition :: $addition")
    println(s"addition :: $subtraction")

  }


}
