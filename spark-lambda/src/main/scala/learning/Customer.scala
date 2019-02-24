package learning

/**
  * @author behzad.pirvali, 8/19/18
  */
class Customer(val name: String, val address: String) {
  def doNothing: Unit = ()
  var id =""
}

object Customer {
  def main(args: Array[String]): Unit = {
      val Eric = new Customer("Eric", "29 Havig road")
      Eric.id= "sss"
      println("Eric:" + Eric)
  }
}