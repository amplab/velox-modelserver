package edu.berkeley.veloxms

import edu.berkeley.veloxms.storage._
import com.typesafe.scalalogging._
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import java.nio.ByteBuffer
import java.io.IOException
import java.net.URLDecoder

import tachyon.TachyonURI;
import tachyon.Pair;
import tachyon.r.sorted.ClientStore;
import tachyon.r.sorted.Utils;

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

import com.google.common.io.{ByteStreams, Closeables}

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit

class MatrixFactorizationModel(
    val numFeatures: Int,
    val modelStorage: ModelStorage[FeatureVector],
    val averageUser: WeightVector,
    val config: VeloxConfiguration) extends Model[Long, FeatureVector]
    with LazyLogging {

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long): FeatureVector = {
    modelStorage.getFeatureData(data) match {
      case Success(features) => features
      case Failure(thrown) => {
        val msg = "Error computing features: " + thrown
        logger.warn(msg)
        throw new Exception(msg)
      }
    }
  }

  def deserializeInput(data: Array[Byte]) : Long = {
    ByteBuffer.wrap(data).getLong
  }

  def getFeatures(item: Long, cache: FeatureCache[Long]): FeatureVector = {
    cache.getItem(item) match {
      case Some(f) => f
      case None => {
        val f = computeFeatures(item)
        cache.addItem(item, f)
        f
      }
    }
  }

  def getWeightVector(userId: Long) : WeightVector = {
    val result: Try[Array[Double]] = modelStorage.getUserFactors(userId)
    result match {
      case Success(u) => u
      case Failure(thrown) => {
        logger.warn("User weight not found: " + thrown)
        averageUser
      }
    }
  }

  def retrainInSpark(sparkMaster: String, trainingData: String) {

    // TODO finish implementing this method

    val numFeatures = 50
    val numIters = 20


    // get jar location: from http://stackoverflow.com/a/6849255/814642
    val path = classOf[MatrixFactorizationModel].getProtectionDomain().getCodeSource().getLocation().getPath()
    val decodedPath = URLDecoder.decode(path, "UTF-8")
    logger.info(s"Jar path: $decodedPath")

    val conf = new SparkConf()
    .setMaster(sparkMaster)
    .setAppName("VeloxRetrainMatrixFact")
    .setJars(List(decodedPath))

    val sc = new SparkContext(conf)
    // val bytesData: RDD[(String, Array[Byte])] = sc.hadoopFile[String, Array[Byte], TachyonKVPartitionInputFormat](trainingData)
    val bytesData: RDD[(String, Array[Byte])] =
        sc.newAPIHadoopFile[String, Array[Byte], TachyonKVPartitionInputFormat](trainingData)

      
    val debugStr = bytesData.map(_._1).collect().mkString(", ")
    logger.info(s"Filenames: $debugStr")
    
    // val data = sc.textFile(trainingData)
    // val sample = data.take(5)
    // val kryo = KryoThreadLocal.kryoTL.get
    // val result = kryo.deserialize(sample(0)).asInstanceOf[HashMap[Long, Double]]
    //
    // val ratings = data.map(_.split("::") match {
    //   case Array(user, item, score, date) => Rating(user.toInt, item.toInt, score.toDouble)
    // })


    // val model = ALS.train(ratings, 50, 20, 1)

    // model.userFeatures.mapPartitions( )//write to Tachyon)

    /*

    val userFeatures = model.userFeatures
    val userFeaturesFlat = userFeatures.map{case (a, b) => (a, b.mkString(","))}
    userFeaturesFlat.saveAsTextFile("userFeatures10M-r1.txt")

    val productFeatures = model.productFeatures
    val productFeaturesFlat = productFeatures.map{case (a, b) => (a, b.mkString(","))}
    productFeaturesFlat.saveAsTextFile("productFeatures10M-r1.txt")
    */

  }

}



object MatrixFactorizationModel {
  val lambda = 1

}


// Adapated from Spark's WholeTextFile{InputFormat,RecordReader}
class TachyonKVPartitionInputFormat extends CombineFileInputFormat[String, Array[Byte]] {
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  override def createRecordReader(
    split: InputSplit,
  context: TaskAttemptContext): RecordReader[String, Array[Byte]] = {

    new CombineFileRecordReader[String, Array[Byte]](
      split.asInstanceOf[CombineFileSplit],
      context,
      classOf[TachyonKVPartitionRecordReader])
}

/**
 * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API.
 */
def setMaxSplitSize(context: JobContext, minPartitions: Int) {
  val files = listStatus(context)
  val totalLen = files.map { file =>
    if (file.isDir) 0L else file.getLen
  }.sum
  val maxSplitSize = Math.ceil(totalLen * 1.0 /
    (if (minPartitions == 0) 1 else minPartitions)).toLong
  super.setMaxSplitSize(maxSplitSize)
  }
}


class TachyonKVPartitionRecordReader(
  split: CombineFileSplit,
  context: TaskAttemptContext,
  index: Integer)
extends RecordReader[String, Array[Byte]] {

  val path = split.getPath(index)
  val fs = path.getFileSystem(context.getConfiguration)

  // True means the current file has been processed, then skip it.
  var processed = false

  val key = path.toString
  var value: Array[Byte] = null

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

  override def close(): Unit = {}

  override def getProgress: Float = if (processed) 1.0f else 0.0f

  override def getCurrentKey: String = key

  override def getCurrentValue: Array[Byte] = value

  override def nextKeyValue(): Boolean = {
    if (!processed) {
      val fileIn = fs.open(path)
      val innerBuffer = ByteStreams.toByteArray(fileIn)
      value = new BytesWritable(innerBuffer).getBytes
      // value = new Text(innerBuffer).toString
      Closeables.close(fileIn, false)
      processed = true
      true
    } else {
      false
    }
  }
}
















