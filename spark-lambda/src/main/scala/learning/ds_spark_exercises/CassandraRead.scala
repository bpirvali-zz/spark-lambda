package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/12/18
  */
object CassandraRead {
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();
    import com.datastax.spark.connector._

    // Spark-Cassandra-API
    val qryPart1 = sc.cassandraTable("killr_video", "videos_by_year_title").select("title")
    val qryPart2 = qryPart1.where("added_year = 2015 and title >= 'T'").limit(5)
    val resultRDD = qryPart2.collect
    resultRDD.foreach(row => println(row.getString("title")))

    // Spark-API
    val rows = sc.cassandraTable("killr_video", "videos_by_year_title")
    val filterYear = rows.filter(record => record.getInt("added_year") == 2015)
    val filterTitle = filterYear.map(record => record.getString("title")).filter(title => title >= "T")
    filterTitle.take(5).foreach(println)

    // 2014 animation movies
    val rows2 = sc.cassandraTable("killr_video", "videos").select("title","genres","release_year")
    val filterGenre = rows2.filter(movie => movie.getList[String]("genres").contains("Animation"))
    val filterYear2 = filterGenre.filter(movie => movie.getInt("release_year") == 2014)
    val output = filterYear2.map( movie => "Title: " + String.format("%1$-26s", movie.getString("title")) + "   " +
      "Year: " + movie.getInt("release_year"))
    output.foreach(println)

    // movie ratings
    val rows3 =sc.cassandraTable("killr_video", "videos").select("mpaa_rating")
    val numRows = rows3.count
    val numRatedG = rows3.filter(row => row.getString("mpaa_rating") == "G").count
    val numRatedPG = rows3.filter(row => row.getString("mpaa_rating") == "PG").count
    val numRatedPG13 = rows3.filter(row => row.getString("mpaa_rating") == "PG-13").count
    val numRatedR = rows3.filter(row => row.getString("mpaa_rating") == "R").count
    val numRatedNR = rows3.filter(row => row.getString("mpaa_rating") == "NR").count
    printf("Percentage of movies rated G     : %.2f%%\n", (100.0 * numRatedG / numRows))
    printf("Percentage of movies rated PG    : %.2f%%\n", (100.0 * numRatedPG / numRows))
    printf("Percentage of movies rated PG-13 : %.2f%%\n", (100.0 * numRatedPG13 / numRows))
    printf("Percentage of movies rated R     : %.2f%%\n", (100.0 * numRatedR / numRows))
    printf("Percentage of movies rated NR    : %.2f%%\n", (100.0 * numRatedNR / numRows))
  }

}
