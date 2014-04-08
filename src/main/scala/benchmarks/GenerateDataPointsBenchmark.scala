import benchmarks.Benchmark
import conf.BenchmarkConfig
import org.apache.spark.SparkContext
import scala.util.Random

object GenerateDataPointsBenchmark extends Benchmark {
  override def run(sc: SparkContext): Unit = {

    val dataPoints = Seq.fill(10000000)(
      (Random.nextInt(4) * 10 + Random.nextInt(10), Random.nextInt(4) * 10 + Random.nextInt(10)));
    sc.parallelize(dataPoints).saveAsTextFile(BenchmarkConfig.HDFS + "/2D/data");
  }
}
