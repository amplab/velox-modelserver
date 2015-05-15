package edu.berkeley.veloxms.models

import breeze.linalg.{Vector, normalize}
import breeze.numerics.exp
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage.BroadcastProvider
import loaders.{LabeledData, NewsgroupsDataLoader}
import nodes.learning.NaiveBayesEstimator
import nodes.misc.{CommonSparseFeatures, TermFrequency}
import nodes.nlp.{LowerCase, NGramsFeaturizer, Tokenizer, Trim}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{UnionRDD, RDD}
import pipelines.Transformer


class NewsgroupsModel(
    val modelName: String,
    val broadcastProvider: BroadcastProvider,
    val averageUser: WeightVector,
    val trainPath: String
  ) extends KeystoneModel[String] {

  val numFeatures = NewsgroupsDataLoader.classes.length
  def fit(sc: SparkContext): Transformer[String, FeatureVector] = {
    val trainDataRaw: RDD[(Int, String)] = new UnionRDD(sc, NewsgroupsDataLoader.classes.zipWithIndex.map {
      case (className, index) => sc.textFile(s"$trainPath/$className").filter(_.trim.length > 0).map(index -> _)
    })

    val trainData = LabeledData(trainDataRaw)
    val numClasses = NewsgroupsDataLoader.classes.length

    // Build the classifier estimator
    logInfo("Training classifier")
    val predictor = Trim.then(LowerCase())
        .then(Tokenizer()).then(new NGramsFeaturizer(1 to 2)).to[Seq[Any]].then(TermFrequency(x => 1))
        .thenEstimator(CommonSparseFeatures(50000)).fit(trainData.data).to[Vector[Double]]
        .thenLabelEstimator(NaiveBayesEstimator(numClasses))
        .fit(trainData.data, trainData.labels).then(x => normalize(exp(x), 1))

    predictor.then(_.toArray)
  }
}
