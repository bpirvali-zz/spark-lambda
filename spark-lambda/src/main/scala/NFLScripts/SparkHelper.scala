package NFLScripts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  * @author Behzad Pirvali, 7/16/18
  */
object SparkHelper {
  /*
  Spark 2.0, SparkSession, a new entry point that subsumes SQLContext and HiveContext
  SparkSession is the entry point for reading data, similar to the old SQLContext.read.
  SparkSession can be used to execute SQL queries over data,
  getting the results back as a DataFrame (i.e. Dataset[Row]).
   */
  private val logger = Logger.getLogger(this.getClass)

  def getAndConfigureSparkSession(): SparkSession = {
    Logger.getLogger("org").setLevel(Level.WARN)      // reduce spark app startup noise

    /*
      https://docs.databricks.com/spark/latest/gentle-introduction/sparksession.html
      Configuration options set in the builder are automatically propagated over to Spark and Hadoop during I/O
    */
    val conf = new SparkConf()
      .setAppName("SparkAppDir2Kafka2Cassandra")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.cassandra.connection.host", "127.0.0.1")      // GUDENKAUFJ-LALM, 127.0.0.1, localhost, "10.198.58.18 staging
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    //$$$ maybe .set("spark.cassandra.output.ignoreNulls", "true")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()              // enableHiveSupport create a hivecontext for runtime CREATE TEMPORARY FUNCTION...
      //          .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
      .config(conf)                   // todo: RV (conf) needed?
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark
  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()      // should get since we call getAndConfigureSparkSession in Main
  }
}
