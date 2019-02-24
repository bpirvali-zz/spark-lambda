package streaming

/**
  * @author behzad.pirvali, 9/28/18
  */
import com.twitter.algebird.HyperLogLogMonoid
import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils.SparkUtils
import functions._
/**
  * Created by Ahmad Alkilani on 5/30/2016.
  */
object StreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = SparkUtils.getSparkContext("Lambda with Spark")
    val sqlContext = SparkUtils.getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)

    val activityStateSpec =
      StateSpec.function(mapActivityStateFunc)
        .timeout(Minutes(120))

    def streamingApp(sc: SparkContext, batchDuration: Duration): StreamingContext = {
      val ssc = new StreamingContext(sc, batchDuration)
      val sqlContext = SparkUtils.getSQLContext(sc)

      val inputPath = SparkUtils.isIDE match {
        case true => "file:///Users/behzad.pirvali/Tools/vagrant/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }
      val textDStream = ssc.textFileStream(inputPath)


      val activityStream = textDStream.transform( input => {
        input.flatMap { line =>
          val record = line.split("\\t")
          val MS_IN_ONE_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_ONE_HOUR * MS_IN_ONE_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      }).cache()

      val statefulActivityByProduct  = activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.createOrReplaceTempView("activity")
        sqlContext.sql("")
        val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour
                                            """)
        activityByProduct.map( r => {
          ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0),r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4)))
        }).rdd
      }).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4),
          filterFunc = (record) => false
        )
        .foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
          .toDF().registerTempTable("ActivityByProduct"))


      // unique visitors by product
      val visitorStateSpec =
        StateSpec
          .function(mapVisitorsStateFunc)
          .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map( a => {
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      } ).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4),
          filterFunc = (record) => false
        ) // only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .toDF().createOrReplaceTempView("VisitorsByProduct"))


      // statefullActivityByProduct.print()
      ssc
    }
    val ssc = SparkUtils.getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}

/*   .updateStateByKey((newItemsPerKey: Seq[ActivityByProduct],
                      currentState: Option[(Long, Long, Long, Long)]) => {


   var (prevTimestemp, purchase_count, add_to_cart_count, page_view_count) =
     currentState.getOrElse(System.currentTimeMillis(), 0L, 0L, 0L)
   var result: Option[(Long, Long, Long, Long)] = null

   if (newItemsPerKey.isEmpty) {
     if (System.currentTimeMillis() - prevTimestemp > 30000 + 4000)
       result = None
     else
       result = Some(prevTimestemp, purchase_count, add_to_cart_count, page_view_count)
   } else {
     newItemsPerKey.foreach(a => {
       purchase_count += a.purchase_count
       add_to_cart_count += a.add_to_cart_count
       page_view_count += a.page_view_count
     })
     result = Some(System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count)
   }
   result
 })
*/
