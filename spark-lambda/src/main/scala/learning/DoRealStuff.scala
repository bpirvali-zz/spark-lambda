package learning

/**
  * @author behzad.pirvali, 8/25/18
  */

trait Stuff {
  def doStuff
}

trait LoggableStuff extends Stuff {
  abstract override def doStuff {
    println("logging enter")
    super.doStuff
    println("logging exit")
  }
}

trait TransactionalStuff extends Stuff {
  abstract override def doStuff {
    println("start TX")
    try {
      super.doStuff
      println("commit TX")
    } catch {
      case e: Exception =>
        println("rollback TX")
    }
  }
}

trait RetryStuff extends Stuff {
  abstract override def doStuff {
    var times = 0
    var retry = true
    while (retry) {
      try {
        super.doStuff
        retry = false
      } catch {
        case e: Exception =>
          if (times < 3) { // retry 3 times
            times += 1
            println("operation failed - retrying in 2 secs...: " + times)
            Thread.sleep(2000);
          } else {
            retry = false
            throw e
          }
      }
    }
  }
}

class RealStuff extends Stuff {
  def doStuff {
    println("doing real stuff...")
    Thread.sleep(1000)
    println("DONE!")
  }
}

class RealStuffWithException extends Stuff {
  def doStuff {
    println("doing real stuff...")
    Thread.sleep(1000)
    throw new RuntimeException("Exception during doing stuff!")
    println("DONE!")
  }
}

object DoRealStuff {
  def main(args: Array[String]): Unit = {
    println("---------------------------------------------------------------------------------")
    println("-- new RealStuff ")
    println("---------------------------------------------------------------------------------")
    val stuff = new RealStuff
    stuff.doStuff

    println("---------------------------------------------------------------------------------")
    println("-- new RealStuff with LoggableStuff ")
    println("---------------------------------------------------------------------------------")
    val stuff2 = new RealStuff with LoggableStuff
    stuff2.doStuff

    println("---------------------------------------------------------------------------------")
    println("-- new RealStuff with TransactionalStuff with LoggableStuff ")
    println("---------------------------------------------------------------------------------")
    val stuff3 = new RealStuff with TransactionalStuff with LoggableStuff
    stuff3.doStuff
//
//    println("---------------------------------------------------------------------------------")
//    println("-- new RealStuff with RetryStuff with TransactionalStuff with LoggableStuff ")
//    println("---------------------------------------------------------------------------------")
//    val stuff4 = new RealStuffWithException` with RetryStuff with TransactionalStuff with LoggableStuff
//    stuff4.doStuff

  }

}
//