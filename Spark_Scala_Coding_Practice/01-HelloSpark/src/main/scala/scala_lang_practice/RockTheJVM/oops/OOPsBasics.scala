package scala_lang_practice.RockTheJVM.oops

/**
 * created by ANEK on Monday 12/27/2021 at 2:51 PM 
 */

object OOPsBasics extends App{


  class Person(name:String,age:Int)  // class Parameter
  { println(s"Person [private variables/members ] class parameters :: ${name}")
  }

  val p  = new Person("anek",10)
  //println(p.age)  => 1. CTE , you can't access class parameter directly


  /* 2. class parameters are NOT FIELDS : you make parameter to field by keyword var or val line :: 18  */

  class Person1( var name:String,val age:Int )
  val p1 = new Person1( "anek",30 )
  println(s"AGE :: ${p1.age} and name is :: ${p1.name}")

  /* 3.       */
  val p2 = new Person2("vicky",45)
  println( s"p2 :: ${p2.name} , ${p2.age} , ${p2.x} ")

  class Person2(val name:String,val age:Int){
    var x = 2
    println("I am in the class body "+x)


    // EXERCISE

    class Writer(first_name:String,last_name:String,val age:Int){
      def  full_name() = s" ${first_name} ${last_name}"

    }
    class Novel(name :String,yearOfPub :Int,author:Writer){

      def checkAuthor(author:Writer) = author == this.author
      def authorAge = yearOfPub - author.age


    }
    val author = new Writer("Charles","Dickens",1812)
    val impersonate = new Writer("Charles","Dickens",1812)
    val novel = new Novel("charles",1862,author)

    println(s"Writer age : ${author.age}")
    println(s"Novel :: ${novel.checkAuthor(impersonate)}")              

  }




}