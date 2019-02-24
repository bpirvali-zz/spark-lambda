package NFLScripts

import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession
/**
  * @author Behzad Pirvali, 7/16/18
  */
object SparkHelper {
  private var sparkSession: SparkSession = null

  def createSparkSession(appName: String, logLevel: String, conf: SparkConf, master: String):
  SparkSession = {
    val sparkBuilder = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .config(conf)

    if (master.length()>0)
      sparkBuilder.master(master)

    sparkSession = sparkBuilder.getOrCreate()
    sparkSession.sparkContext.setLogLevel(logLevel)
    sparkSession
  }

  def getSparkSession() = {
    if (sparkSession==null)
      throw new Exception("sparkSession does not exist, try calling createSparkSession first!")
    else
      sparkSession
  }
}
