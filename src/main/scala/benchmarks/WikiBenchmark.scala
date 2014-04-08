package benchmarks
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import conf.BenchmarkConfig

object WikiBenchmark extends Benchmark{
  override def run(sc: SparkContext): Unit = {

    val englishPages = sc.textFile(BenchmarkConfig.HDFS + "/wiki/pagecounts")
                          .filter(_.split("\\s+")(1) == "en") //filter by english pages
                          .cache

    println("Filtered " + englishPages.count()+ " Wikipedia pages in English.");

    englishPages.map(line => {
                              val Array(dateTime, pageCode, pageTitle, numHits, pageSize) = line.split("\\s+");
                              (pageTitle, numHits.toInt);
                              })
                .reduceByKey(_+_)
                .map(x => (x._2, x._1))
                .sortByKey(false)
                // .collect
                .saveAsTextFile(BenchmarkConfig.HDFS + "/wikipedia/topEnglishSites");
    /*stop Spark context */
    sc.stop()
  }

}
