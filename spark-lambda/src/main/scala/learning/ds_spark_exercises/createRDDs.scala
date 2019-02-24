package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/11/18
  */
object createRDDs {
  def main(args: Array[String]): Unit = {
    val sc = createSparkContext()

    // manual-create
    val list = List("Winnie the Pooh", "The Tigger Movie", "Pirates of the Caribbean", "Apollo 13", "Mallcop",
    "Mockingjay - Part 1", "The Good Dinosaur", "Lava", "The Peanuts Movie")

    val rddList = sc.parallelize(list)
    rddList.take(3).foreach(println)



    import com.datastax.spark.connector._
    val tableRDD = sc.cassandraTable("test", "movies_by_actor")

    tableRDD.foreach(println)

    val playlist = List(("The Martian", 141, 7.6),
    ("Bridge of Spies", 141, 8.0),
    ("The Imitation Game", 113, 8.0),
    ("The Wolf of Wall Street", 180, 7.9),
    ("Creed", 132, 8.6),
    ("John Wick", 101, 7.2),
    ("The Hundred-Foot Journey", 122, 7.3))

    val playlistRDD = sc.parallelize(playlist)

    val playlistCount = playlistRDD.count
    println("Count: " + playlistCount)

    val playlistTotal = playlistRDD.map(movie => movie._2).reduce{case (x,y) => x + y}
    println("Total Duration: " + playlistTotal + " minutes" )

    val playlistAverage = playlistRDD.map(movie => movie._3).reduce{case (x,y) => x + y} / playlistCount
    println("Average Rating: " + playlistAverage)
  }
}
