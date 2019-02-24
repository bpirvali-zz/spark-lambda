package streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils

/**
  * @author Behzad Pirvali, 10/11/18
  *
  * Extends StreamingJob1 with re-creating ssc from a checkpoint directory in case of a failure
  */
object StreamingJob2 {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = SparkUtils.getSparkContext("Lambda with Spark")
    val sqlContext = SparkUtils.getSQLContext(sc)

    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration): StreamingContext = {
      val ssc = new StreamingContext(sc, batchDuration)
      val inputPath = SparkUtils.isIDE match {
        case true => "file:///Users/behzad.pirvali/Tools/vagrant/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }
      val textDStream = ssc.textFileStream(inputPath)
      textDStream.print()
      ssc
    }

    val ssc = SparkUtils.getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
