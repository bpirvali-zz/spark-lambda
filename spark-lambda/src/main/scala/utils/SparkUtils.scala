package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Behzad Pirvali, 7/14/18
  */
object SparkUtils {
  def getSparkContext(appName: String) = {

    val isIDE = ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IdeaIC2018") ||
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")

    var checkpointDirectory = ""
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")
      .set("spark.executor.memory","1g")

    // Check if running from IDE
    if (isIDE) {
      conf.setMaster("local[2]")
    }

    // setup spark context
    val sc = new  SparkContext(conf)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

}
