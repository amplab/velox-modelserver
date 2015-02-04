package edu.berkeley.veloxms.pipelines

import java.io._

import edu.berkeley.veloxms.pipelines.estimators.{MulticlassNaiveBayesEstimator, MostFrequentSparseFeatureSelector}
import edu.berkeley.veloxms.pipelines.transformers.{SimpleNGramTokenizer, MulticlassClassifierEvaluator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object NewsgroupsPipeline {

  def main(args : Array[String]) {
    // Set up all the contexts

    val conf = new SparkConf().setAppName("classifier").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val numClasses = 20

    val newsByDateFolder: String = "/Users/tomerk11/Downloads/20news-bydate"

    val trainData = List(
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/alt.atheism").map(0 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/comp.graphics").map(1 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/comp.os.ms-windows.misc").map(2 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/comp.sys.ibm.pc.hardware").map(3 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/comp.sys.mac.hardware").map(4 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/comp.windows.x").map(5 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/misc.forsale").map(6 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/rec.autos").map(7 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/rec.motorcycles").map(8 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/rec.sport.baseball").map(9 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/rec.sport.hockey").map(10 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/sci.crypt").map(11 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/sci.electronics").map(12 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/sci.med").map(13 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/sci.space").map(14 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/soc.religion.christian").map(15 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/talk.politics.guns").map(16 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/talk.politics.mideast").map(17 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/talk.politics.misc").map(18 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-train/talk.religion.misc").map(19 -> _._2)
    ).reduceLeft(_.union(_))

    val testData = List(
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/alt.atheism").map(0 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/comp.graphics").map(1 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/comp.os.ms-windows.misc").map(2 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/comp.sys.ibm.pc.hardware").map(3 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/comp.sys.mac.hardware").map(4 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/comp.windows.x").map(5 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/misc.forsale").map(6 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/rec.autos").map(7 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/rec.motorcycles").map(8 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/rec.sport.baseball").map(9 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/rec.sport.hockey").map(10 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/sci.crypt").map(11 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/sci.electronics").map(12 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/sci.med").map(13 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/sci.space").map(14 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/soc.religion.christian").map(15 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/talk.politics.guns").map(16 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/talk.politics.mideast").map(17 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/talk.politics.misc").map(18 -> _._2),
      sc.wholeTextFiles(newsByDateFolder + "/20news-bydate-test/talk.religion.misc").map(19 -> _._2)
    ).reduceLeft(_.union(_))

    // Build the classifier estimator
    val predictionPipelineEstimator = Pipeline.Pipeline() andThen
        SimpleNGramTokenizer andThen
        new MostFrequentSparseFeatureSelector(100000) andThen
        new MulticlassNaiveBayesEstimator(numClasses)

    // Train the predictionPipeline
    println("Training classifier")
    val predictionPipeline = predictionPipelineEstimator.fit(trainData)

    val attachInt = new Transformer[String, (Int, String)] {
      override def transform(in: String): (Int, String) = (0, in)
    }
    val removeInt = new Transformer[(Int, Vector), Vector] {
      override def transform(in: (Int, Vector)): Vector = in._2
    }
    val pipeline = attachInt andThen predictionPipeline andThen removeInt

    // Serialize and deserialize the prediction pipeline
    val fos = new FileOutputStream("./data/news-classifier.tmp")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(pipeline)
    oos.close()

    /*val fis = new FileInputStream("./data/news-classifier.tmp")
    val ois = new ObjectInputStream(fis)
    val loadedPredictionPipeline = ois.readObject().asInstanceOf[Transformer[(Int, String), (Int, Vector)]]
    ois.close()*/

    // Evaluate the classifier
    println("Evaluating classifier")
    val evaluator = (x: RDD[(Int, String)]) => MulticlassClassifierEvaluator
        .evaluate(x.mapPartitions(predictionPipeline.transform))
    //logger.info("Train Dataset:\n" + evaluator(trainData).mkString("\n"))
    println("Test Dataset:\n" + evaluator(testData).mkString("\n"))

    sc.stop()
  }

}
