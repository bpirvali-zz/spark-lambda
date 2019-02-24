package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Behzad Pirvali, 7/14/18
  */
object SparkUtils {
  val isIDE = _isRunningInIDE
  def getSparkContext(appName: String) = {
    var checkpointDirectory = ""

    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)

    // Check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "F:\\Libraries\\WinUtils") // required for winutils
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///tmp"
    } else {
      //checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
      checkpointDirectory = "hdfs://localhost:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }
  private def _isRunningInIDE: Boolean = {
    //println("_isRunningInIDE:" + ManagementFactory.getRuntimeMXBean.getInputArguments.toString)
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA") ||
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("-agentlib:jdwp")
  }

  /*
  * creates a ssc from a checkpoint OR
  * creates a new ssc from your streaming app
  * */
  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc: () => StreamingContext = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

}
