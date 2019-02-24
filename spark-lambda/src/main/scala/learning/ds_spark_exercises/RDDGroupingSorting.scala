package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object RDDGroupingSorting {
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._

//    val videos = sc.cassandraTable[(String, Int, String)]("killr_video",
//      "videos_by_tag").select("tag", "release_year", "title")
//
//    videos.map(r => ((r._1, r._2), r._3)).groupByKey().take(5).foreach(println)


    val videos = sc.cassandraTable[(String, Int, String)]("killr_video",
      "videos_by_tag").select("tag", "release_year", "title")

    videos.map(r => ((r._1, r._2), 1)).reduceByKey(_ + _).map(r =>
      (r._2, r._1)).sortByKey(false).take(5).foreach(println)
  }

}
