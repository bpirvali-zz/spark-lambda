package learning.patternmatching

object Main {
  val me = Student("Behzad", "Pirvali", 123)
  val p1 = new Person("Havig", "Havigian")

  def getFullID[T<: Person](somebody: T)= {
    somebody match {
      case Student(fname,lname, id) => s"$fname-$lname-$id"
      case p: Person => p.fullName
    }
  }

  def incIfInteger(x: Any): Any = {
    x match {
      case y: Int => y + 1
      case y: Any => y
    }
  }

  val fraction = new PartialFunction[Int, Int] {
    def apply(d: Int) = 42 / d
    def isDefinedAt(d: Int) = d != 0
  }

  val fraction2: PartialFunction[Int, Int] =
  { case d: Int if d != 0 ⇒ 42 / d }

  def main(args: Array[String]): Unit = {
    println(getFullID(me))
    println(getFullID(p1))
  }
}
