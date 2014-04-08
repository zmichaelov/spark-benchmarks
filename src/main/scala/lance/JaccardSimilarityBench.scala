package lance

import org.apache.spark.SparkContext
import conf.BenchmarkConfig

/**
 * Created by zmichaelov on 3/30/14.
 */
object JaccardSimilarityBench {
  def main(args: Array[String]) {
    /* Memory dedicated to RDD's versus Java */
    System.setProperty("spark.storage.memoryFraction", "0.3")

  	/* SparkContext tells Spark how to access cluster */
    val sc = new SparkContext("local", "JaccardSimilarityBench")
    val rawTfVectors = sc.textFile(BenchmarkConfig.HDFS + "/tfvect.txt")
    val cleanedTfVectors =
        rawTfVectors.map(doc => {
        val Array(docKey, vect) = doc.split(" ");
        val wordDictKeys = vect.substring(1,vect.length - 1) //weed off { }
                               .split(",")
                               .map(_.split("=")(0)) //get each word key
                               .toSet
        (docKey, wordDictKeys, wordDictKeys.size);
        }).cache;

    cleanedTfVectors.cartesian(cleanedTfVectors) // for every combination of documents
                    .map(docPair =>{
                        val (doc1, doc2) = docPair
                        val wordsInCommon = (doc1._2 & doc2._2).size.toDouble
                        val pseudoJaccardSimilarity = wordsInCommon / (doc1._3 + doc2._3 - wordsInCommon)
                        (doc1._1, doc2._1, pseudoJaccardSimilarity)
                        })
                    .foreach(println(_))

    /*stop Spark context */
    sc.stop()
  }
}
