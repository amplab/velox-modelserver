package edu.berkeley.veloxms.pipelines.transformers

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Created by tomerk11 on 10/26/14.
 * TODO: Clean this up a lot
 */
object MulticlassClassifierEvaluator {
  def evaluate(in: RDD[(Int, Vector)]): Seq[(String, Double)] = {
    val confusionMatrixRowMap = in.mapValues(_.toArray).mapValues(x => if (x.sum == 0) x ++ Array(1d) else x ++ Array(0d)).reduceByKeyLocally((a, b) => a.zip(b).map(x => x._1 + x._2))
    val numClasses = confusionMatrixRowMap.toSeq.head._2.size
    val confusionMatrixArray = (0 until numClasses).map(confusionMatrixRowMap.getOrElse(_, new Array[Int](numClasses))).toArray

    println(confusionMatrixArray.deep.mkString("\n"))

    val stats = in.map(p => {
      (0 until p._2.size).map(n => {
        val tp = if ((p._1 == n) && (p._2(n) == 1)) 1d else 0d
        val fp = if ((p._1 != n) && (p._2(n) == 1)) 1d else 0d
        val tn = if ((p._1 != n) && (p._2(n) != 1)) 1d else 0d
        val fn = if ((p._1 == n) && (p._2(n) != 1)) 1d else 0d
        (tp, fp, tn, fn)
      })
    })
    val classAggregatedStats = stats.reduce((x, y) => {
      x.zip(y).map(z => (
          z._1._1 + z._2._1,
          z._1._2 + z._2._2,
          z._1._3 + z._2._3,
          z._1._4 + z._2._4))
    })
    val finalClassStats = classAggregatedStats.map(x => {
      val totTP = x._1
      val totFP = x._2
      val totTN = x._3
      val totFN = x._4

      val precision : Double = if ((totTP+totFP)==0) 1.0 else totTP/(totTP+totFP)
      val recall : Double = if ((totTP+totFN)==0) 1.0 else totTP/(totTP+totFN)
      val F1 : Double = if (recall+precision==0) 0.0 else (2*recall*precision)/(recall+precision)
      val accuracy : Double = (totTP+totTN)/(totTP+totFN+totFP+totTN)
      (precision, recall, F1, accuracy)
    })

    val classStatsOutputSeq = finalClassStats.zipWithIndex.flatMap(x => {
      val label = x._2
      Seq(
        (s"Class $label precision", x._1._1),
        (s"Class $label recall", x._1._2),
        (s"Class $label F1", x._1._3),
        (s"Class $label accuracy", x._1._4))
    })

    val aggregatedStats = classAggregatedStats.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
    val totTP = aggregatedStats._1
    val totFP = aggregatedStats._2
    val totTN = aggregatedStats._3
    val totFN = aggregatedStats._4

    val precision : Double = if ((totTP+totFP)==0) -1.0 else totTP/(totTP+totFP)
    val recall : Double = if ((totTP+totFN)==0) -1.0 else totTP/(totTP+totFN)
    val F1 : Double = (2*recall*precision)/(recall+precision)
    val accuracy : Double = (totTP+totTN)/(totTP+totFN+totFP+totTN)

    val macroF1 = finalClassStats.aggregate(0d)((x, y) => x + y._3, (x, y) => x + y) / finalClassStats.size
    classStatsOutputSeq ++ Seq(
      ("micro precision", precision),
      ("micro recall", recall),
      ("micro F1", F1),
      ("macro F1", macroF1),
      ("micro accuracy", accuracy))

    //finalClassStats
  }
}
