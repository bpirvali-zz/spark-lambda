package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object PairRDDSetOperations {
  case class ActorYear(actor_name: String, release_year: Int)
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    //import com.datastax.spark.connector._

    // union
    val A = sc.parallelize(Array(("k1","v1"), ("k1","w1"), ("k2","v2"), ("k1","v3"), ("k3","v4")))
    val B = sc.parallelize(Array(("k1","w1"), ("k2","w2"), ("k2","w3"), ("k4","w4")))
    A.union(B).foreach(println)


  }

}
