package edu.berkeley.veloxms.spark


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

// TODO abstract out how to do IO to/from persistent storage in Spark
class RetrainMatrixFactorizationModel(sparkMaster: String) {


  def retrain(trainingData: String) {

    val numFeatures = 50
    val numIters = 20
    val lambda = 1

    val sc = new SparkContext(sparkMaster, "RetrainVeloxModel")

    val data = sc.textFile(trainingData)

    val ratings = data.map(_.split("::") match {
      case Array(user, item, score, date) => Rating(user.toInt, item.toInt, score.toDouble)
    })

    val model = ALS.train(ratings, 50, 20, 1)



  }







}

