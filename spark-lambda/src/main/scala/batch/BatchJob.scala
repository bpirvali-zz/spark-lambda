package batch

import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object BatchJob {
  def main(args: Array[String]): Unit = {
    // get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")
      .set("spark.executor.memory","1g")

    // Check if running from IDE
    if ( ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IdeaIC2018") ||
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    ) {
      conf.setMaster("local[2]")
    }


    // setup spark context
    val sc = new  SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // initialize input RDD
    //val sourceFile = "/Users/behzad.pirvali/Tools/vagrant/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant"
    //val sourceFile = "file:///Users/behzad.pirvali/Tools/vagrant/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant"
    val sourceFile = "file:///vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    // spark action results in job execution
    //input.foreach(println);

    val inputRDD = input.flatMap { line =>
      val record = line.split("\\t")
      val MS_IN_ONE_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_ONE_HOUR * MS_IN_ONE_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    // defining a UDF
    //sqlContext.udf.register("Underexposed", (pageViewCount: Long, purchaseCount: Long) => if (purchaseCount==0) 0 else pageViewCount/purchaseCount)

    val inputDF = inputRDD.toDF()
    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.createOrReplaceTempView("activity")

    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        | FROM activity
        | GROUP BY product, timestamp_hour
      """.stripMargin
    )

    val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://127.0.0.1:9000/lambda/batch1")

    activityByProduct.createOrReplaceTempView("activityByProduct")

    visitorsByProduct.foreach( t=>println(t) )
    activityByProduct.foreach( t=>println(t) )
  }
}
