package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/13/18
  */
object BroadcastVars {
  case class Video(title : String, description : Option[String])

  def main(args: Array[String]): Unit = {
    val sc = createSparkContext();
    import com.datastax.spark.connector._

    //val superheroes = Set("Batman", "Superman", "Wolverine")
    val superheroes = sc.broadcast( Set("Batman", "Superman", "Wolverine") )
    def descriptionContainsSuperhero(vid : Video) : Boolean =
    {
      for(w <- vid.description.getOrElse("").split(' '))
      {
        if(superheroes.value.contains(w))
        {
          return true;
        }
      }
      return false;
    }

    val videos = sc.cassandraTable[Video]("killr_video", "videos")
    videos.filter( descriptionContainsSuperhero ).collect.foreach(record => println(record + "\n"))
  }


}
