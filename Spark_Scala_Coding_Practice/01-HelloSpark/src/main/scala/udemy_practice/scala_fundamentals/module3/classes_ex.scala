package udemy_practice.scala_fundamentals.module3
object classes_ex {
  def main(args: Array[String]): Unit = {

     val emp = new Emp("Anek")
     println( emp.deptIs(2) )

    println(emp deptIs 3)
    
  }


}

class Emp(name :String){
  println(s"Constructor is called ")
  println(s"printing name : ${name}")
  println(s"modified name : ${name+"_update"}")

  println(s"dept call from constructor : ${deptIs(11)}" )


  def deptIs(int : Int):String = name+int

}
class EmpW {
  println(s"Constructor is called ")      // Here it does not print

  println(s"Constructor is called again")

  def deptIs(int : Int):Int = int

}