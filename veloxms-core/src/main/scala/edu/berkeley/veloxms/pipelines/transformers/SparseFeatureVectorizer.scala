package edu.berkeley.veloxms.pipelines.transformers

import edu.berkeley.veloxms.pipelines.Transformer
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * A transformer which given a feature space, maps extracted features of the form (name, value) into a sparse vector
 */
class SparseFeatureVectorizer(featureSpace: Map[String, Int]) extends Transformer[(Int, Seq[(String, Double)]), (Int, Vector)] {
  override def transform(in: (Int, Seq[(String, Double)])): (Int, Vector) = {
    val features = in._2.map(f => (featureSpace.get(f._1), f._2))
          .filter(_._1.isDefined)
          .map(f => (f._1.get, f._2))
    (in._1, Vectors.sparse(featureSpace.size, features))
  }
}
