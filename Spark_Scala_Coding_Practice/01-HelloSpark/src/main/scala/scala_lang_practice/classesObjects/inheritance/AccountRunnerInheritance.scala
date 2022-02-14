package scala_lang_practice.classesObjects.inheritance

import java.time.LocalDateTime
import java.util.UUID
 class Account(id :UUID,name:String,date:LocalDateTime){

  private val _id :UUID = id
  private var _name:String = name
  private val _date:LocalDateTime = date

  // Auxilliary  Constructor
  def this(name :String ) = this(UUID.randomUUID(),name,LocalDateTime.now)

  def getId :UUID = _id
  def getName :String = _name
  def getDate : LocalDateTime = _date
  def updateName(newName :String ) : Unit = _name = newName

   
}

 class CreditAccount(creditAccName :String ) extends Account(creditAccName :String){
   private val _accountType :String = creditAccName
   def getAccountType :String = _accountType
   

}
class DepositAccount(creditAccName :String = "*****") extends Account(creditAccName :String ){
  private val _accountType :String = creditAccName
  def getAccountType :String = _accountType


}

object AccountRunnerInheritance extends App{
  val acc1 = new Account(UUID.randomUUID(),"Account",LocalDateTime.now)
  acc1.updateName("UpdatedAccount")

  val accAux = new Account("AuxAccount")

  println(acc1.getId,acc1.getName,acc1.getDate.getYear)
  println(accAux.getId,accAux.getName,accAux.getDate)

  val creditAccount = new CreditAccount("CreditAccount")
  println(s"credit  :: ${creditAccount.getAccountType} , ${creditAccount.getName} , ${creditAccount.getDate}")

  val creditAccount1 = new DepositAccount()
  println(s"deposit  :: ${creditAccount1.getAccountType} , ${creditAccount1.getName} , ${creditAccount1.getDate}")
}
