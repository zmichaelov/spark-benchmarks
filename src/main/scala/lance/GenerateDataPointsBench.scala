package lance

import org.apache.spark.{SparkContext, SparkConf}
import conf.BenchmarkConfig
import scala.util.Random

/**
 * Created by zmichaelov on 3/30/14.
 */
object GenerateDataPointsBench {
  def main(args: Array[String]) {
    /* Memory dedicated to RDD's versus Java */
    System.setProperty("spark.storage.memoryFraction", "0.5")

  	/* SparkContext tells Spark how to access cluster */
    // val sc = new SparkContext("local", "GenerateDataPointsBench")

    val sparkConf = new SparkConf()
    sparkConf.setMaster(BenchmarkConfig.SPARK_MASTER)
    sparkConf.setAppName("GenerateDataPointsBench")
    sparkConf.set("spark.executor.memory", "1g")
    sparkConf.setJars(List("target/scala-2.10/sparkproto_2.10-0.1.jar"))

    /* SparkContext in distributed mode: */
    val sc = new SparkContext(sparkConf);

    val dataPoints = Seq.fill(10000000)(
        (Random.nextInt(4)*10 + Random.nextInt(10),Random.nextInt(4)*10 + Random.nextInt(10)));
    sc.parallelize(dataPoints).saveAsTextFile(BenchmarkConfig.HDFS + "/2D/data");

    /*stop Spark context */
    sc.stop()
  }
}
