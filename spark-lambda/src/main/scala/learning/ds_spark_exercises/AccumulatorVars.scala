package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/13/18
  */
object AccumulatorVars {
  //case class Video( description : Option[String] )

  def main(args: Array[String]): Unit = {
    val sc = createSparkContext();
    import com.datastax.spark.connector._

    var numVideos = sc.accumulator(0)
    var numVideosWithoutDescription = sc.accumulator(0)

    sc.cassandraTable("killr_video", "videos").foreach{ v =>
      numVideos += 1
      if ( v.getStringOption("description").isEmpty ) numVideosWithoutDescription += 1 }

    println("numVideos:" + numVideos.value)
    println("numVideos without comment:" + numVideosWithoutDescription.value)
    printf( "%.2f%%", 100.0 * numVideosWithoutDescription.value.asInstanceOf[Float] / numVideos.value.asInstanceOf[Float] )
  }


}
