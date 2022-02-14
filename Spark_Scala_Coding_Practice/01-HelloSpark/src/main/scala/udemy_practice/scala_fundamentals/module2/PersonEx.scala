package udemy_practice.scala_fundamentals.module2

object PersonEx {
  def main(args: Array[String]): Unit = {


    val person = Person("Anek",100)
    println(person.weightInPounds)
    println(person.weightInKilos)

    person.weightInKilos = 600
    person.weightInKilos = (500)
    println(person.weightInPounds)



  }

}

case class Person(name : String ,var weightInPounds :Double){

  def weightInKilos:Double = weightInPounds / 2

  def weightInKilos_=(newWeight :Double):Unit = {
    weightInPounds = (newWeight) / 2
  }

}