import benchmarks.TwitterBenchmark
import java.io.{PrintWriter, File}
import org.apache.hadoop.metrics2.source.JvmMetricsSource
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import scala.util.Random

object BenchmarkRunner {
  val parallelism = "spark.default.parallelism";
  val storageFraction = "spark.storage.memoryFraction";
  val shuffleFraction = "spark.shuffle.memoryFraction";
  val shuffleCompress= "spark.shuffle.compress";// true/false
  val shuffleSpillCompress = "spark.shuffle.spill.compress";// true/false
  val rddCompress = "spark.rdd.compress";// true/false
  val akkaThreads = "spark.akka.threads";// 4 8 16 32

  val cores = 16;
  val trials = 25;

  var parallel = cores;
  var storage = 6;
  var shuffle = 3;

  val random = new Random();

  def writeHeaders(writer: PrintWriter) {
    writer.write(parallelism + "," + storageFraction + "," + shuffleFraction + ",time\n"); // write headers
  }

  def writeRow(writer: PrintWriter, runTime: Long, sparkConf: SparkConf) {
    writer.write(
      sparkConf.get(parallelism) + "," +
        sparkConf.get(storageFraction) + "," +
        sparkConf.get(shuffleFraction) + "," +
        runTime / 1000.0 + "\n");
  }

  def pickParameters() {
    val oldStorage = storage;
    do {
      storage = random.nextInt(8) + 1;
    } while (storage == oldStorage)
    shuffle = 9 - storage;

    parallel = cores * 3;
    //    val oldpar = parallel;
    //    do {
    //      val temp = random.nextInt(9) + 1;
    //      parallel = cores*temp;
    //    } while (parallel == oldpar)
  }

  def initSparkConf(appName: String, parallel: Int, storage: Int, shuffle: Int): SparkConf = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("spark://sail01.egr.duke.edu:10010")
    sparkConf.setAppName(appName);
    sparkConf.set("spark.executor.memory", "4g")
    sparkConf.set(parallelism, Integer.toString(parallel));
    sparkConf.set(storageFraction, storage / 10.0 + "");
    sparkConf.set(shuffleFraction, shuffle / 10.0 + "");
    sparkConf.setJars(List("target/scala-2.10/benchmarks-benchmark-runner_2.10-1.0.jar"))
    return sparkConf;
  }

  def main(args: Array[String]) {
    // change this to whichever benchmark you want to run
    val benchmark = TwitterBenchmark;

    val name = benchmark.getClass().getCanonicalName();
    val writer = new PrintWriter(new File(name + ".csv"));

    writeHeaders(writer);
    /* SparkContext tells Spark how to access cluster */
    val statsReporter = new StatsReportListener;
    //for (run <- 1 to trials) {
    for (parallel <- 4 to 20) {
      for (storage <- 1 to 8) {
        for (shuffle <- 1 to 8) {
          if (storage + shuffle < 9) {
            val sparkConf = initSparkConf(name, parallel, storage, shuffle);
            val sc = new SparkContext(sparkConf);
            sc.addSparkListener(statsReporter);
            benchmark.run(sc);
            //SparkEnv.get.metricsSystem.registerSource(new JvmMetricsSource())

            sc.stop()
            val runTime = System.currentTimeMillis() - sc.startTime;
            writeRow(writer, runTime, sparkConf);
          }
        }
      }
    }
    //pickParameters();
    //}
    writer.close();
  }
}
