package NFLScripts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

/**
  * @author Behzad Pirvali, 7/16/18
  */
object Driver {
  lazy val logger = Logger.getLogger(this.getClass)

  // spark conf
  val conf = new SparkConf()
  conf.set("spark.executor.memory","1g")

  // spark session
  val spark = SparkHelper.createSparkSession("app-name", "WARN", conf, "local[*]")

  def main(args: Array[String]): Unit = {
    logger.info(s"SparkAppName(${spark.sparkContext.appName}), spark.version(${spark.version})")
    println(s"SparkAppName(${spark.sparkContext.appName}), spark.version(${spark.version}), SparkSession(${spark})")

    val clusterName="la3_stg_cass"
    val mapCassClusterParams = Map(
      "spark.cassandra.connection.host" -> "10.198.58.18,10.198.58.19,10.198.58.20"
      , "spark.cassandra.auth.username" -> "cassandra"
      , "spark.cassandra.auth.password" -> "DataStax#1"
    )


    val cassObj =
      CassandraHelper.Builder()
        .setSparkSession(spark)
        .setGlobalConfig(CassandraHelper.mapGlobalParamsDefault)
        .setClusterConfig(clusterName, CassandraHelper.mapClusterParamsDefault ++ mapCassClusterParams)
        .build()

    //val dfTables = cassObj.readTable("system_schema", "tables")
    cassObj.createTempViewForTable("system_schema", "tables", "vs_tables")
    val sql =
      """SELECT *
        |FROM vs_tables
        |where keyspace_name='nfl_assets'
        |and type like '%frozen<udt_%>'
        |and table_name in ('person', 'player')
        |order by table_name""".stripMargin

    spark.sql("select * from vs_tables where keyspace_name='nfl_assets'").show(1)
  }
}

