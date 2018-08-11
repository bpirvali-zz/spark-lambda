package NFLScripts

import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import com.myco.tools.driver.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

import scala.language.implicitConversions
// cluster configs
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.ConfigParameter


/**
  * @author Behzad Pirvali, 7/16/18
  */
object createViews {

  //val spark = SparkHelper.getAndConfigureSparkSession()
  val spark =     SparkSession
    .builder()
    .getOrCreate()      // should get since we call getAndConfigureSparkSession in Main

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  var sql = ""
  val DataFrameReader  = spark.read.format("org.apache.spark.sql.cassandra")             // org.apache.spark.sql.DataFrameReader
  var logical_DC = ("", "")
  var clusterAuth   = scala.collection.mutable.Map[String, String]()
  var keyspaceTable = scala.collection.mutable.Map[String, String]()
  var viewName = ""

  val cassandraPassword_stg = "DataStax#1"
  val cassandraPassword_prd = "DataPtax#1"

  lazy val solr_stg_la3_system_traces: scala.collection.mutable.Map[String,String] = {
    import com.datastax.driver.core.ConsistencyLevel
    logical_DC = ("solr-stg-la3-system-traces", "10.198.58.21,10.198.58.22,10.198.58.23")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128)
      ++ ReadConf.ConsistencyLevelParam.option(ConsistencyLevel.ONE)  )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_stg)
  }

  //mutable.Map("keyspace" -> "push", "table" -> "push_device")
  lazy val spark_stg_la3: scala.collection.mutable.Map[String,String] = {
    logical_DC = ("spark-stg-la3", "10.198.58.25,10.198.58.26,10.198.58.27")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_stg)  //  "pushdown" -> "true",
  }

  //mutable.Map("keyspace" -> "push", "table" -> "push_device")
  lazy val cassandra_stg_la3: scala.collection.mutable.Map[String,String] = {
    logical_DC = ("cassandra-stg-la3", "10.198.58.18,10.198.58.19,10.198.58.20")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_stg)  // "pushdown" -> "true",
  }

  //mutable.Map("keyspace" -> "nfl_assets", "table" -> "player_stats")
  lazy val solr_stg_la3: scala.collection.mutable.Map[String,String] = {
    logical_DC = ("solr-stg-la3", "10.198.58.21,10.198.58.22,10.198.58.23")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_stg)
  }


  lazy val solr_prd_la3_system_traces: scala.collection.mutable.Map[String,String] = {
    import com.datastax.driver.core.ConsistencyLevel
    logical_DC = ("solr-prd-la3-system-traces", "10.198.59.20,10.198.59.21,10.198.59.22,10.198.59.23,10.198.59.24")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128)
      ++ ReadConf.ConsistencyLevelParam.option(ConsistencyLevel.ONE)  )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_prd)
  }
  println(s"\nUSE THE spark_<env> logical DC clusters when ever possible to do read/writes instead of the main ring. Mutations will be replicated to cassandra_<env> ring\n")
  //mutable.Map("keyspace" -> "push", "table" -> "push_device")
  lazy val spark_prd_la3: scala.collection.mutable.Map[String,String] = {
    logical_DC = ("spark-prd-la3", "10.198.59.25,10.198.59.26,10.198.59.27")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_prd) // "pushdown" -> "true",
  }
  /*
    mutable.Map("keyspace" -> "push", "table" -> "push_device")
    desc push;
    //CREATE KEYSPACE push WITH replication = {'class': 'NetworkTopologyStrategy', 'cassandra-prd-la3': '3', 'spark-prd-la3': '3'}  AND durable_writes = true;

   */
  lazy val cassandra_prd_la3: scala.collection.mutable.Map[String,String] = {
    logical_DC = ("cassandra-prd-la3", "10.198.59.12,10.198.59.13,10.198.59.14,10.198.59.15,10.198.59.31,10.198.59.241,10.198.59.242,10.198.59.243,10.198.59.244,10.198.59.245")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_prd)   // "pushdown" -> "true",
  }
  //mutable.Map("keyspace" -> "nfl_assets", "table" -> "player_stats")  port 9042
  lazy val solr_prd_la3: scala.collection.mutable.Map[String,String] = {
    // csshx jgudenkauf@10.198.59.20 jgudenkauf@10.198.59.21 jgudenkauf@10.198.59.22 jgudenkauf@10.198.59.23 jgudenkauf@10.198.59.24"
    logical_DC = ("solr-prd-la3", "10.198.59.20,10.198.59.21,10.198.59.22,10.198.59.23,10.198.59.24")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises", "spark.cassandra.auth.password" -> cassandraPassword_prd)
  }
  // localhost cassandra replication = 1
  lazy val localhost: scala.collection.mutable.Map[String,String] = {
    logical_DC = ("localhost", "127.0.0.1")
    spark.setCassandraConf(logical_DC._1, CassandraConnectorConf.ConnectionHostParam.option(logical_DC._2) ++ ReadConf.SplitSizeInMBParam.option(128) )
    scala.collection.mutable.Map("cluster" -> logical_DC._1, "spark.cassandra.auth.username" -> "ds_spark_exercises")
  }

  import spark.implicits._   // needed for df as[tables]
  case class table (keyspace_name: String, table_name: String)
  var systemTable = scala.collection.mutable.Map[String, String]()

  def createViews(cluster: String, keyspaceName: String, clusterAuth: scala.collection.mutable.Map[String,String], cassandraOld: Boolean = true) = {
    if (cassandraOld) {
      systemTable("systemKeyspaceName") =  "system"
      systemTable("systemTableName") =  "schema_columnfamilies"
      systemTable("systemColumnName") = "columnfamily_name"
    }
    else {
      systemTable("systemKeyspaceName") =  "system_schema"
      systemTable("systemTableName") =  "tables"
      systemTable("systemColumnName") = "table_name"
    }
    val keyspaceTable = scala.collection.mutable.Map("keyspace" -> systemTable("systemKeyspaceName"), "table" -> systemTable("systemTableName"))
    DataFrameReader.options(clusterAuth ++ keyspaceTable).load().createOrReplaceTempView("table_v") //where keyspace_name like "${keyspace}"
    ///  spark.sql("""select * from table_v limit 1""").show()
    // like clause is not working with solr indexes so I am using an IN clause when needed
    val sql_cmd = s"""select keyspace_name, ${systemTable("systemColumnName")} as table_name from table_v where keyspace_name = ("${keyspaceName}") """
    ///val df = spark.sql(sql_cmd).as[table]
    val df = spark.sql(sql_cmd).as[table]
    //  df.take(1).foreach(println)
    df.collect().map(row => {
      val keyspaceTableNameView = scala.collection.mutable.Map("keyspace" -> keyspaceName, "table" -> row.table_name)
      val viewName = cluster + "__" + keyspaceName + "__" + row.table_name
      //println(viewName)
      println("spark.sql(\"SELECT COUNT(*) AS CNT_" + viewName + " FROM " + viewName + "\").show();" )
      DataFrameReader.options(clusterAuth ++ keyspaceTableNameView).load().createOrReplaceTempView(viewName)
    }
    )
  }

  def main(args: Array[String]): Unit = {
    createViews("spark_stg_la3", "nfl_assets", spark_stg_la3)

  }
}
