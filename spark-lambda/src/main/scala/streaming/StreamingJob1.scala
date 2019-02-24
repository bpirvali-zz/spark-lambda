package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.SparkUtils

/**
  * @author Behzad Pirvali, 10/11/18
  */
object StreamingJob1 {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = SparkUtils.getSparkContext("Lambda with Spark")
    val sqlContext = SparkUtils.getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)
    val ssc = new StreamingContext(sc, batchDuration)
    val inputPath = SparkUtils.isIDE match {
      case true => "file:///Users/behzad.pirvali/Tools/vagrant/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
      case false => "file:///vagrant/input"
    }
    val textDStream = ssc.textFileStream(inputPath)
    textDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
