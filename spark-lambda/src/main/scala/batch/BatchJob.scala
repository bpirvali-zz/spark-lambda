package batch

import java.lang.management.ManagementFactory

import config.Settings
import domain.Activity
import org.apache.spark.sql.{SaveMode, SparkSession}

object BatchJob {
  def main(args: Array[String]): Unit = {

    // create spark session
    val sparkBuilder = SparkSession
      .builder()
      .appName("Lambda with Spark")
      .config("spark.executor.memory","1g")
      //.master("local[*]")
      .enableHiveSupport()
      //.getOrCreate()

    var sourceFile = "file:///vagrant/data.tsv"
    val runningLocal=if ( ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IdeaIC2018") ||
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA") ||
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("-agentlib:jdwp")
    ) true else false

    if (runningLocal) {
      sourceFile = "/Users/behzad.pirvali/Tools/vagrant/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val wlc = Settings.WebLogGen

    // initialize input RDD
    val input = sc.textFile(sourceFile)

    val inputRDD = input.flatMap { line =>
      val record = line.split("\\t")
      val MS_IN_ONE_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_ONE_HOUR * MS_IN_ONE_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    import spark.implicits._

    val inputDF = inputRDD.toDF()

    // create temp tables
    inputDF.createOrReplaceTempView("activity")



    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        | FROM activity
        | GROUP BY product, timestamp_hour
        | ORDER BY unique_visitors DESC
      """.stripMargin
    )

    //visitorsByProduct.show(10)
    val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour
                                            """).cache()


    if (!runningLocal)
      activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://127.0.0.1:9000/lambda/batch1")

    visitorsByProduct.foreach( t=>println(t) )
    activityByProduct.foreach( t=>println(t) )
  }
}
