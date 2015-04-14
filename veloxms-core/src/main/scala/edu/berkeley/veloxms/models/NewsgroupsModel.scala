package edu.berkeley.veloxms.models

import java.io.{ObjectInputStream, FileInputStream}

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.pipelines.transformers.{SimpleNGramTokenizer, MulticlassClassifierEvaluator}
import edu.berkeley.veloxms.pipelines._
import edu.berkeley.veloxms.pipelines.estimators.{MulticlassNaiveBayesEstimator, MostFrequentSparseFeatureSelector}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import edu.berkeley.veloxms.util.{EtcdClient, Logging}


class NewsgroupsModel(
    val name: String,
    val etcdClient: EtcdClient,
    val modelLoc: String,
    val numFeatures: Int,
    val averageUser: WeightVector,
    val cacheResults: Boolean,
    val cacheFeatures: Boolean,
    val cachePredictions: Boolean
  ) extends Model[String, FeatureVector] with Logging {

    val defaultItem: FeatureVector = Array.fill[Double](numFeatures)(0.0)

  private val initialModel: Transformer[String, FeatureVector] = {
    val fis = new FileInputStream(modelLoc)
    val ois = new ObjectInputStream(fis)
    val loadedPredictionPipeline = ois.readObject().asInstanceOf[Transformer[String, Vector]]
    ois.close()
    loadedPredictionPipeline andThen new Transformer[Vector, FeatureVector] {
      override def transform(in: Vector): FeatureVector = in.toArray
    }
  }

  private val modelBroadcast = broadcast[Transformer[String, FeatureVector]]("model")

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  override def computeFeatures(data: String, version: Version): FeatureVector = {
    modelBroadcast.get(version).get.transform(data)
  }

  override protected def retrainFeatureModelsInSpark(observations: RDD[(UserID, String, Double)], nextVersion: Version): RDD[(String, FeatureVector)] = {
    val conf = new SparkConf().setAppName("classifier").setMaster("local[4]")//.setJars(SparkContext.jarOfObject(this).toSeq)

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
    val nextModel = attachInt andThen predictionPipeline andThen removeInt andThen new Transformer[Vector, FeatureVector] {
      override def transform(in: Vector): FeatureVector = in.toArray
    }

    modelBroadcast.put(nextModel, nextVersion)

    // Evaluate the classifier
    println("Evaluating classifier")
    val evaluator = (x: RDD[(Int, String)]) => MulticlassClassifierEvaluator
        .evaluate(x.mapPartitions(predictionPipeline.transform))
    //logger.info("Train Dataset:\n" + evaluator(trainData).mkString("\n"))
    println("Test Dataset:\n" + evaluator(testData).mkString("\n"))

    sc.stop()

    val features = observations.map(_._2).distinct()
    features.zip(features.mapPartitions(x => nextModel.transform(x)))
  }
}
