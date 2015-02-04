package edu.berkeley.veloxms.pipelines.transformers

import edu.berkeley.veloxms.pipelines.Transformer

/**
 * Created by tomerk11 on 10/26/14.
 */
object SimpleNGramTokenizer extends Transformer[(Int, String), (Int, Seq[(String, Double)])] {
  val ns = Array(1,2,3)

  override def transform(in: (Int, String)): (Int, Seq[(String, Double)]) = (in._1, getNgrams(in._2).distinct.map((_, 1d)))

  def getNgrams(text: String): Seq[String] = {
    val unigrams = text.trim.toLowerCase.split("[\\p{Punct}\\s]+")
    ns.map(n => {
      unigrams.sliding(n).map(gram => gram.mkString(" "))
    }).flatMap(identity)
  }
}
