package scala_lang_practice.parameterizedTypes

import scala_lang_practice.parameterizedTypes.HttpUtils.HttpVerb

import scala.util.Success

object HttpUtils{

  // A sealed trait can be extended only in the same file as its declaration.
  sealed trait HttpVerb
  case object GET extends HttpVerb
  case object PUT extends HttpVerb
  case object POST extends HttpVerb

  def asHttpVerb(httpVerb: String) : HttpVerb = {
    httpVerb.toLowerCase match {
      case "get" => GET
      case "put" => PUT
      case "post" => POST
      case _ => throw new IllegalArgumentException(s"No method is available .")
    }

  }

}


object CommonParameterizedTypes {
  import scala.util.{Try,Success,Failure}
  def main(args: Array[String]): Unit = {

    def toHttpVerb(httpVerb:String):Option[HttpVerb] =
      Try{
        HttpUtils.asHttpVerb(httpVerb)
      } match {
        case Success(value) => Some(value)
        case Failure(exception) => None         
      }

    def toHttpVerb2(httpVerb:String):Either[Throwable,HttpVerb] =
      Try{
        HttpUtils.asHttpVerb(httpVerb)
      } match {
        case Success(value) => Right(value)
        case Failure(exception) => Left(exception)
      }
    println(toHttpVerb2("post"))
    println(toHttpVerb2("pot"))

  }
}
