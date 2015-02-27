package edu.berkeley.veloxms.models

import edu.berkeley.veloxms._
// import java.io.IOException
// import java.net.URLDecoder
// import java.nio.ByteBuffer
// import scala.collection.JavaConversions._
// import scala.util.{Try,Success,Failure}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.util.{Failure, Success}
// import org.apache.hadoop.fs.Path
// import org.apache.hadoop.io.{BytesWritable,NullWritable}
// import org.apache.hadoop.mapreduce.{InputSplit,JobContext}
// import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
// import org.apache.hadoop.mapreduce.lib.input.{CombineFileRecordReader,CombineFileSplit}
// import org.apache.hadoop.mapreduce.{RecordReader,TaskAttemptContext}
// import org.apache.spark._
// import org.apache.spark.mllib.recommendation.{ALS,Rating}
// import org.apache.spark.rdd.RDD
// import org.apache.spark.SparkContext._
import edu.berkeley.veloxms.storage._

class MatrixFactorizationModel(
    val numFeatures: Int,
    itemStorage: ModelStorage[Long, FeatureVector],
    val userStorage: ModelStorage[Long, WeightVector],
    val observationStorage: ModelStorage[Long, Map[Long, Double]],
    val averageUser: WeightVector,
    val cacheResults: Boolean,
    val cacheFeatures: Boolean,
    val cachePredictions: Boolean
  ) extends Model[Long, FeatureVector] {

    val defaultItem: FeatureVector = Array.fill[Double](numFeatures)(0.0)

  // val logger = Logger(LoggerFactory.getLogger(classOf[MatrixFactorizationModel]))

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long): FeatureVector = {
    itemStorage.get(data) match {
      case Some(features) => features
      case None => {
        val msg = "Error computing features"
        logWarning(msg)
        throw new Exception(msg)
      }
    }
  }

  /**
   * THIS DOESN'T WORK YET!!!
   * Retrains the model in the provided Spark cluster
   */
  def retrainInSpark(sparkMaster: String) {
    val sparkHome = "/root/spark"
    logWarning("Starting spark context")
    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("VeloxOnSpark!")
      .setJars(SparkContext.jarOfObject(this).toSeq)
      .setSparkHome(sparkHome)
      // .set("spark.akka.logAkkaConfig", "true")
    val sc = new SparkContext(conf)
    logWarning("Parallelizing data")
    val data = sc.parallelize((0 to 5000))
    logWarning(s"Counting data ${data.count}")
    logWarning(s"Top is: ${data.top(10)}")
    sc.stop()
    println("Done")

  }
  //
  //   val sparkHome = "/root/spark"
  //   logWarning("Starting spark context")
  //   val sc = new SparkContext(sparkMaster, "SparkTestApp", sparkHome,
  //       SparkContext.jarOfObject(this).toSeq)
  //   logWarning("Parallelizing data")
  //   val data = sc.parallelize((0 to 5000))
  //   logWarning(s"Counting data ${data.count}")
  //   logWarning(s"Top is: ${data.top(10)}")
  //   sc.stop()
  //   logWarning("Done")
  // }
  //
    // TODO finish implementing this method
    //
    // val numFeatures = 50
    // val numIters = 20
    //
    // //Had to comment out because of storage refactoring
    // val trainingData = ""//s"${conf.tachyonMaster}/${conf.ratingsStoreName}"
    //
    //
    //
    // // get jar location: from http://stackoverflow.com/a/6849255/814642
    // val path = classOf[MatrixFactorizationModel]
    //   .getProtectionDomain()
    //   .getCodeSource()
    //   .getLocation()
    //   .getPath()
    // val decodedPath = URLDecoder.decode(path, "UTF-8")
    // logInfo(s"Jar path: $decodedPath")
    //
    // val sparkConf = new SparkConf()
    // .setMaster(sparkMaster)
    // .setAppName("VeloxRetrainMatrixFact")
    // .setJars(List(decodedPath))
    //
    // val sc = new SparkContext(sparkConf)
    // // val bytesData: RDD[(String, Array[Byte])] = 
    // //    sc.hadoopFile[String, Array[Byte], TachyonKVPartitionInputFormat](trainingData)
    // logInfo("Created spark context")
    // val bytesData: RDD[(String, Array[Byte])] =
    //     sc.newAPIHadoopFile[String, Array[Byte], TachyonKVPartitionInputFormat](trainingData)
    // logInfo(s"Read ${bytesData.count} partitions")
    //
    //
    //
    //   
    // val debugStr = bytesData.map(_._1).collect().mkString(", ")
    // logInfo(s"Filenames: $debugStr")
    
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

  // }

}


// Adapated from Spark's WholeTextFile{InputFormat,RecordReader}
// class TachyonKVPartitionInputFormat extends CombineFileInputFormat[String, Array[Byte]] {
//   override protected def isSplitable(context: JobContext, file: Path): Boolean = false
//
//   override def createRecordReader(
//     split: InputSplit,
//   context: TaskAttemptContext): RecordReader[String, Array[Byte]] = {
//
//     new CombineFileRecordReader[String, Array[Byte]](
//       split.asInstanceOf[CombineFileSplit],
//       context,
//       classOf[TachyonKVPartitionRecordReader])
// }
//
// /**
//  * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API.
//  */
// def setMaxSplitSize(context: JobContext, minPartitions: Int) {
//   val files = listStatus(context)
//   val totalLen = files.map { file =>
//     if (file.isDir) 0L else file.getLen
//   }.sum
//   val maxSplitSize = Math.ceil(totalLen * 1.0 /
//     (if (minPartitions == 0) 1 else minPartitions)).toLong
//   super.setMaxSplitSize(maxSplitSize)
//   }
// }
//
//
// class TachyonKVPartitionRecordReader(
//   split: CombineFileSplit,
//   context: TaskAttemptContext,
//   index: Integer)
// extends RecordReader[String, Array[Byte]] {
//
//   val path = split.getPath(index)
//   val fs = path.getFileSystem(context.getConfiguration)
//
//   // True means the current file has been processed, then skip it.
//   var processed = false
//
//   val key = path.toString
//   var value: Array[Byte] = null
//
//   override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}
//
//   override def close(): Unit = {}
//
//   override def getProgress: Float = if (processed) 1.0f else 0.0f
//
//   override def getCurrentKey: String = key
//
//   override def getCurrentValue: Array[Byte] = value
//
//   override def nextKeyValue(): Boolean = {
//     if (!processed) {
//       val fileIn = fs.open(path)
//       val innerBuffer = ByteStreams.toByteArray(fileIn)
//       value = new BytesWritable(innerBuffer).getBytes
//       // value = new Text(innerBuffer).toString
//       Closeables.close(fileIn, false)
//       processed = true
//       true
//     } else {
//       false
//     }
//   }
// }
//
//














