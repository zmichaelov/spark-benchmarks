package benchmarks

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TwitterBenchmark extends Benchmark {
  override def run(sc: SparkContext): Unit = {

    val rawTwitterRDD = sc.textFile("hdfs://sail05.egr.duke.edu:10005/twitter/twitter.tsv")
      .map(_.split("\\s+")) //split by white space
      .map(a => a(1)) //extract twitter screen name
      .filter(_ != "") //filter empty entries
    //.cache()
    ;

    println("Processing " + rawTwitterRDD.count() + " Twitter screen names");

    val concatenatedNames = rawTwitterRDD.flatMap(_.sliding(5).map((_, 1)))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .saveAsTextFile("hdfs://sail05.egr.duke.edu:10005/twitter/top5gramNames");
  }

}
