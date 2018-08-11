package scratch

object TestMultipleParamsList {
  def sayHello(name: String)(implicit whoAreYou: ()=>String) = {
    println(s"Hello $name, my name is ${whoAreYou()}")
  }

  implicit def provideName() = {"Scala"}

  def main(args: Array[String]): Unit = {
    //sayHello("test"){()=>"Havig"}
    sayHello("test")
  }
}
