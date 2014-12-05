package edu.berkeley.veloxms.pipelines.estimators

import edu.berkeley.veloxms.pipelines.transformers.SparseFeatureVectorizer
import edu.berkeley.veloxms.pipelines.{Transformer, Estimator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * A simple feature selector that chooses all features produced by a sparse feature extractor,
 * and produces a transformer which builds a sparse vector out of all the features it extracts
 */
class MostFrequentSparseFeatureSelector(numFeatures: Int) extends Estimator[(Int, Seq[(String, Double)]), (Int, Vector)] with Serializable {
  override def fit(data: RDD[(Int, Seq[(String, Double)])]): Transformer[(Int, Seq[(String, Double)]), (Int, Vector)] = {
    val featureSpace = data.flatMap(_._2.map(_._1)).countByValue().toSeq.sortBy(-_._2).take(numFeatures).map(_._1).zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}