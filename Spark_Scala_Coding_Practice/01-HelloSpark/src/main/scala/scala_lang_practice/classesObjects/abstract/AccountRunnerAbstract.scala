package scala_lang_practice.classesObjects.`abstract`

import java.time.LocalDateTime
import java.util.UUID
 abstract class Account(id :UUID,name:String,date:LocalDateTime){

  private val _id :UUID = id
  private var _name:String = name
  private val _date:LocalDateTime = date
  val _accountType : String 
  // Auxilliary  Constructor
  def this(name :String ) = this(UUID.randomUUID(),name,LocalDateTime.now)

  def getId :UUID = _id
  def getName :String = _name
  def getDate : LocalDateTime = _date
  def updateName(newName :String ) : Unit = _name = newName
  def getAccountType : String = _accountType

}
 class CreditAccount(creditAccName :String ) extends Account(creditAccName :String){
   val _accountType : String = "Credit"
  // def getAccountType : String = _accountType

 }

object AccountRunnerAbstract extends App{

  val creditAccount : Account = new CreditAccount("CreditAccount")
  println(s"credit ==>  ${creditAccount.getAccountType} , ${creditAccount.getName} , ${creditAccount.getDate}")

 }
