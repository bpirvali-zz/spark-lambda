package scratch.patternmatching

object Main {
  val me = Student("Behzad", "Pirvali", 123)
  val p1 = new Person("Havig", "Havigian")

  def getFullID[T<: Person](somebody: T)= {
    somebody match {
      case Student(fname,lname, id) => s"$fname-$lname-$id"
      case p: Person => p.fullName
    }
  }

  def main(args: Array[String]): Unit = {
    println(getFullID(me))
    println(getFullID(p1))
  }
}
