package learning.ds_spark_exercises

/**
  * @author behzad.pirvali, 8/13/18
  */
object CassandraWrite {
  case class Video( added_year : Int,
                     title : String,
                     video_id : java.util.UUID,
                     added_date : java.util.Date,
                     avg_rating : Option[Float],
                     description : Option[String],
                     user_id : java.util.UUID )

  case class reorderedVideo( title : String,
                             added_year : Int,
                             added_date : java.util.Date,
                             avg_rating : Option[Float],
                             description : Option[String],
                             user_id : java.util.UUID,
                             video_id : java.util.UUID )

  def main(args: Array[String]): Unit = {
    val sc = createSparkContext();
    import com.datastax.spark.connector._

    val videos = sc.cassandraTable("killr_video", "videos_by_year_title").as(Video).where("added_year = 2014")

    val filterRating = videos.filter(
      video =>
        if (video.avg_rating.isEmpty)
          false
        else video.avg_rating.get <= 4
    )
    println(filterRating.count)
    filterRating.foreach(println)

    // save to a new table
    // BAD way
    filterRating.saveAsCassandraTable("killr_video", "worst_2014_videos",
      SomeColumns("added_year","title","video_id", "added_date","avg_rating", "description","user_id")
    )
    // read from the new table
    // spark chooses the wrong PK --> only one row returns!!!!
    val worstVideos = sc.cassandraTable("killr_video", "worst_2014_videos").select("title","added_year","avg_rating")
    worstVideos.collect.foreach(println)

    // save to a new table
    // good way
    import com.datastax.spark.connector.cql._;
    import com.datastax.spark.connector.types._;

    // define the table
    val tableDef = TableDef( "killr_video", "worst_2014_videos_ex",
      Seq(new ColumnDef("title", PartitionKeyColumn, TextType)),
      Seq(new ColumnDef("added_year", ClusteringColumn(0), IntType)),
      Seq(new ColumnDef("added_date", RegularColumn, TimestampType),
        new ColumnDef("avg_rating", RegularColumn, FloatType),
        new ColumnDef("description", RegularColumn, TextType),
        new ColumnDef("user_id", RegularColumn, UUIDType),
        new ColumnDef("video_id", RegularColumn, UUIDType))
    )

    // create new reorderedVideo-columns
    val mappedColumns = filterRating.map(m => reorderedVideo(m.title, m.added_year, m.added_date, m.avg_rating,
      m.description, m.user_id, m.video_id))
    println(mappedColumns.count())

    // save the objects into the table using the table-definition
    mappedColumns.saveAsCassandraTableEx(tableDef)

    // read the rows back
    val worstVideosEx = sc.cassandraTable("killr_video", "worst_2014_videos_ex").select("title","added_year","avg_rating")
    worstVideosEx.collect.foreach(println)
  }

}
