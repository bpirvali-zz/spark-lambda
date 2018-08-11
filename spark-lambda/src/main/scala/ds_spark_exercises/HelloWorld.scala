package ds_spark_exercises

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author Behzad Pirvali, 8/10/18
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext();

    //val sourceFile = "file:///vagrant/data.tsv"
    val cur_dir = System.getProperty("user.dir")
    val sourceFile = s"file:///$cur_dir/spark-lambda/src/main/resources/video-years.csv"
    val records = sc.textFile(sourceFile)
    val years = records.flatMap(record => record.split(",").drop(2))
    val counts = years.map(year => (year,1)).reduceByKey{case (x,y) => x + y}
//    val counts = years.map(year => (year,1)).reduceByKey{ (x,y) => x + y}
    counts.collect().foreach(println)
  }
}
