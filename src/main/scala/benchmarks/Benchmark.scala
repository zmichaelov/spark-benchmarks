package benchmarks

import org.apache.spark.SparkContext

/**
 * Created by zmichaelov on 3/31/14.
 */
trait Benchmark {
  def run(sc: SparkContext)
}
