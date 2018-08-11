import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Behzad Pirvali, 8/10/18
  */
package object ds_spark_exercises {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("ds-spark-exercises")
      .set("spark.executor.memory","1g")
      .setMaster("local[*]")

    val sc = new  SparkContext(conf)
    sc.setLogLevel("ERROR")
    return sc
  }
}
