package learning

/**
  * @author behzad.pirvali, 8/21/18
  */


class Animal extends Comparable[Animal] {
  override def compareTo(o: Animal): Int = 0
}
class Lion extends Animal
class Zebra extends Animal

class DriverAnimal {

}
object DriverAnimal {
  // A extends comparable U, where U is a super class to A
  def sort[A <: Comparable[U], U >: A](List: List[A]) = {}
  //def sort[A <: Lion with Comparable[Animal]](List: List[A]) = {}

  def add1(n: Int): Int = n + 1

  def main(args: Array[String]): Unit = {
    var lions = List[Lion]()
    lions = new Lion +: lions
    lions = new Lion +: lions
    sort[Lion,Animal](lions)

    var zebras = List[Zebra]()
    zebras = new Zebra +: zebras
    zebras = new Zebra +: zebras
    sort[Zebra,Animal](zebras)

    var zoo = List[Animal]()
    zoo = new Zebra +: zoo
    zoo = new Lion +: zoo
    zoo = new Lion +: zoo
    sort(zoo)

    val f = add1 _
    val z: Int => Int = add1
    println("DONE!")
  }
}


