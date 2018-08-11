package NFLScripts

import org.apache.log4j.{Level, Logger}

/**
  * @author Behzad Pirvali, 7/16/18
  */
object Driver {
  lazy val logger = Logger.getLogger(this.getClass)
  //logger.setLevel(Level.TRACE)
  //^ no need to set this inline code since we now use Dlog4j.configuration=file:/...

  val spark = SparkHelper.getAndConfigureSparkSession()

  def main(args: Array[String]): Unit = {
    /*
      We set the apache.log4j logger properties for Spark, this App, etc.. in this App project at:
      /Users/jack.gudenkauf/projects/spark/spark_cassandra/conf/log4j.properties
      The log4j.properties file is set in IntelliJ Run\Edit Configurations VM options
      -Dspark.app.name=SparkAppDir2Kafka2Cassandra -Dspark.master=local[*] -Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dlog4j.configuration=file:/Users/jack.gudenkauf/projects/spark/spark_cassandra/conf/log4j.properties
      ^ note that the spark VM settingd (e.g., spark.app.name) are set in our SparkHelper SparkConf()
     */
    //  https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
    //val spark = SparkHelper.getAndConfigureSparkSession()
    logger.info(s"SparkAppName(${spark.sparkContext.appName}), spark.version(${spark.version}), SparkSession(${spark})")
    println(s"SparkAppName(${spark.sparkContext.appName}), spark.version(${spark.version}), SparkSession(${spark})")
    logger.info(s"streams.awaitAnyTermination()")
  }
}

