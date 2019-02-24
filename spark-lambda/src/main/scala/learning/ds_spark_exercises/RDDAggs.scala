package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object RDDAggs {
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._

    val videos = sc.cassandraTable("killr_video", "videos_by_tag").select("tag")
    //videos.foreach(println)
    videos.map(record => (record.getString("tag"), 1)).reduceByKey(_ + _).collect.take(10).foreach(println)
    videos.map(record => (record.getString("tag"), 1)).reduceByKey((x,y) => x + y ).collect.take(10).foreach(println)
  }

}
