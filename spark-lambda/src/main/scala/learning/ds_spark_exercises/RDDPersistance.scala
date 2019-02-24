package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object RDDPersistance {
  case class Video(title : String, description : Option[String])
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._

    val cassandraVideos = sc.cassandraTable[Video]("killr_video", "videos_by_tag").where("tag = 'christmas'").cache;
    println(cassandraVideos.filter(vid => vid.description.getOrElse("").contains("Santa")).count)
    println(cassandraVideos.filter(vid => vid.description.getOrElse("").contains("gift")).count)
  }

}
