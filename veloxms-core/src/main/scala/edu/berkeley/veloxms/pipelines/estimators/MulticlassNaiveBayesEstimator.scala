package edu.berkeley.veloxms.pipelines.estimators

import edu.berkeley.veloxms.pipelines.{Estimator, Transformer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * An estimator which learns a multiclass naive bayes model from training data and outputs a node that applies the model
 * @param lambda The lambda parameter to use for the various naive bayes models
 */
class MulticlassNaiveBayesEstimator(numClasses: Int, lambda: Float = 1.0f) extends Estimator[(Int, Vector), (Int, Vector)] {
  override def fit(in: RDD[(Int, Vector)]): Transformer[(Int, Vector), (Int, Vector)] = {
    val labeledPoints = in.map(x => LabeledPoint(x._1, x._2))
    val model = NaiveBayes.train(labeledPoints, lambda)

    new Transformer[(Int, Vector), (Int, Vector)] {
      override def transform(vector: (Int, Vector)): (Int, Vector) = {
        // https://issues.scala-lang.org/browse/SI-7005
        (vector._1, Vectors.sparse(numClasses, Seq((model.predict(vector._2).toInt, 1d))))
      }
    }
  }
}