package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object RDDJoiningWithCassandra {
  case class ActorYear(actor_name: String, release_year: Int)
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._

    val actors2014 = sc.parallelize(List(ActorYear("Johnny Depp",2014),
      ActorYear("Bruce Willis",2014)))

//    actors2014.joinWithCassandraTable("killr_video","videos_by_actor").takeSample(false,10).foreach(println)
//    actors2014.joinWithCassandraTable("killr_video","videos_by_actor").on(SomeColumns("actor_name","release_year")).takeSample(false,10).foreach(println)

    sc.cassandraTable("killr_video", "actor").joinWithCassandraTable("killr_video","videos_by_actor").takeSample(false, 10).foreach(println)
  }

}
