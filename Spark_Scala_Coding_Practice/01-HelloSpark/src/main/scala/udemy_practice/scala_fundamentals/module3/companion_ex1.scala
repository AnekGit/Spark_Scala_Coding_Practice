package udemy_practice.scala_fundamentals.module3

import java.io.FileInputStream
import java.util.Properties

object companion_ex1 {
  def main(args: Array[String]): Unit = {

    println( app_config.config.getProperty("log4j.rootCategory") )

    println( app_config("log4j.appender.console.target") )

  }

}

object app_config{

  println("********  calling default constructor code **********")
  val prop : Properties = new Properties()
  prop.load(new FileInputStream("log4j.properties"))
  println(" *********  end of constructor **********************")

  def config = prop

  //def apply(path : String): app_config = new app_config()

  def apply( key : String ) : String = prop.getProperty(key)
}
