package lance

import org.apache.spark.SparkContext
import org.apache.spark.util.Vector
import scala.Vector

/**
 * Created by zmichaelov on 3/30/14.
 */
object KMeansBench {
  def main(args: Array[String]) {
    /* Memory dedicated to RDD's versus Java */
    System.setProperty("spark.storage.memoryFraction", "0.5")

  	/* SparkContext tells Spark how to access cluster */
    val sc = new SparkContext("local", "KMeansBench")

    // based on dataset, cluster is set to have 16 groups a priori
    val k = 16;
    // tolerance
    val convergeDist = 1e-6

    val dataPoints = sc.textFile("hdfs://localhost:9000/2D/data").map(
        line => {
            // parse datapoint
            val dp = line.substring(1,line.length -1)
                         .split(",");
            new Vector(Array(dp(0).toDouble,dp(1).toDouble));
            }
        ).cache;

    println("Processing " + dataPoints.count()+" datapoints");

    //draw random 16 samples
    var centroids = dataPoints.takeSample(false, k, 13);
    var tempDist = 1.0;
    var iter = 0;
    do {
      var closest = dataPoints.map(x =>(closestPoint(x, centroids), x))

      // group by closest centroid piont
      var pointsGroup = closest.groupByKey()

      // generate new centroids
      var newCentroids = pointsGroup.mapValues(ps => average(ps)).collectAsMap()
      tempDist = 0.0
      for (i <- 0 until k) {
        tempDist += centroids(i).squaredDist(newCentroids(i))
      }
      // Assign newCentroids to centroids
      for (newP <- newCentroids) {
        centroids(newP._1) = newP._2
      }
      iter += 1
    } while (tempDist > convergeDist)

    println("KMeans took " + iter + " iterations to converge")

    // the K-means are contained in centroids
    val numArticles = 10
    // TODO, fix poor performance => repeatedly iterating through all datapoints
    for((centroid, index) <- centroids.zipWithIndex) {
      dataPoints.filter(x => closestPoint(x, centroids) == index)
                .take(numArticles)
                .foreach(x => println(x))
    }

    /*stop Spark context */
    sc.stop()
  }

  /* return index of closest candidate vector to point p*/
  def closestPoint(p: Vector, canditates: Array[Vector]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until canditates.length) {
      val tempDist = p.squaredDist(canditates(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    return bestIndex
  }

  /* euclidian average of coordinates */
  def average(ps: Seq[Vector]) : Vector = {
    val numVectors = ps.size
    var out = new Vector(ps(0).elements)
    for (i <- 1 until numVectors) {
      out += ps(i)
    }
    out / numVectors
  }

}
