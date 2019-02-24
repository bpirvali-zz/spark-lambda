package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object RDDPairs {
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._

    val maxLength = 16
    val videos = sc.cassandraTable[(java.util.UUID, String)]("killr_video", "videos").select("video_id", "title")
//    videos.map(record => (
//      record._1,
//      if(record._2.length < maxLength)
//        record._2
//      else
//        record._2.substring(0, maxLength)
//    )
//    ).collect.foreach(println)

    videos.mapValues(title => if(title.length < maxLength) title else title.substring(0, maxLength)).collect(
    ).foreach(println)
  }

}
