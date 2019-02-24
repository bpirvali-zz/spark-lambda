package NFLScripts

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.SparkSession

/**
  * @author behzad.pirvali, 8/31/18
  */
class CassandraHelper private (val clusterName: String, val sparkSession: SparkSession) {

  def readTable(ks: String, table_name: String) = {
    sparkSession.read.format("org.apache.spark.sql.cassandra").options(
      Map("cluster" -> clusterName, "keyspace" -> ks, "table" -> table_name)
    ).load()
  }

  def createTempViewForTable(ks: String, table_name: String, view_name: String) = {
    sparkSession.read.format("org.apache.spark.sql.cassandra").options(
      Map("cluster" -> clusterName, "keyspace" -> ks, "table" -> table_name)
    ).load().createOrReplaceTempView(view_name)
  }
}

object CassandraHelper {
  def Builder(): CassandraBuilder = {
    new CassandraBuilder()
  }

  def build(clusterName: String, sparkSession: SparkSession): CassandraHelper = {
    new CassandraHelper(clusterName, sparkSession)
  }

  val mapGlobalParamsDefault = Map(
    "spark.cassandra.connection.keep_alive_ms" -> "10000"
  )
  val mapClusterParamsDefault = Map(
    "spark.cassandra.input.split.size_in_mb" -> "128"
  )
}

class CassandraBuilder {
  private var sparkSession: SparkSession = null;
  private var mapGlobalConfig: Map[String, String] = null;
  private var clusterName: String = null;
  private var mapClusterConfig: Map[String, String] = null;

  def setSparkSession(sparkSession: SparkSession): CassandraBuilder = {
    this.sparkSession = sparkSession;
    this
  }

  def setGlobalConfig(mapGlobalConfig: Map[String, String]): CassandraBuilder = {
    this.mapGlobalConfig=mapGlobalConfig
    this
  }

  def setClusterConfig(clusterName: String, mapClusterConfig: Map[String, String]): CassandraBuilder = {
    this.clusterName=clusterName
    this.mapClusterConfig=mapClusterConfig
    this
  }

  def build(): CassandraHelper = {
    if (sparkSession==null)
      throw new Exception("Spark session is not set, try calling setSparkSession first!")

    if (mapGlobalConfig==null)
      throw new Exception("cassandra global-config is not set, try calling setGlobalConfig first!")

    if (clusterName==null)
      throw new Exception("cluster-name is not set, try calling setClusterConfig first!")

    if (mapClusterConfig==null)
      throw new Exception("cassandra cluster-config is not set, try calling setClusterConfig first!")

    // setting global config
    sparkSession.setCassandraConf( mapGlobalConfig )

    // setting cassandra config
    sparkSession.setCassandraConf( clusterName, mapClusterConfig )

    CassandraHelper.build( clusterName, sparkSession )
  }
}

