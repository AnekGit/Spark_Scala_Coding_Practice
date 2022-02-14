package udemy_practice.scala_fundamentals.functions
object function_literals {
  def main(args: Array[String]): Unit = {

    val d = "String"
    val f:(String,String)=>String = (in1:String,in2 :String) => in1+"--"+in2

    val f2:(String)=>String = (in1:String) => in1+"--"
    
    val list = List("Anek","Vicky","Nick")
    // Place holder syntax makes it possible to remove the list of parameters
    list.map(_.toUpperCase()).foreach(println)

    val numners = List(1,2,3,4,5,6,7,8,9)
    println(numners.reduce( (sum:Int, b:Int)  => sum + b))

    // Place holder syntax makes it possible to remove the list of parameters

    println( numners.reduce(_ + _) )







  }
  def concate(in1 : String,in2 : String ): String = {
    in1+"_"+in2
  }

}
