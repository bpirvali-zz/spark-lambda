package learning.for_comprehension

import s4j.scala.chapter21.{Customer, Customers}

/**
  * @author behzad.pirvali, 8/23/18
  */
class MyCustomers extends Customers {
  override def add(Customer: Customer): Unit = ???

  override def find(name: String): Option[Customer] = ???

  override def findOrNull(name: String): Customer = ???

  override def iterator: Iterator[Customer] = ???
}
