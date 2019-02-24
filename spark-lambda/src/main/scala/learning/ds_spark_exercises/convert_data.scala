package learning.ds_spark_exercises

/**
  * @author Behzad Pirvali, 8/10/18
  */
object convert_data {
  case class Video( added_year : Int, title : String, description : String )

  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._


    val rows = sc.cassandraTable[Video]("killr_video", "videos_by_year_title").where("added_year=2009")
    val filter = rows.filter(movie => movie.description.toLowerCase.contains("dog"))
    val output = filter.map( movie => "Title: " + String.format("%1$-26s", movie.title) + "\n" +
      "Title: " + movie.title + "\n" +
      "Description: " + movie.description + "\n")
    output.collect.foreach(println)



  }
}
