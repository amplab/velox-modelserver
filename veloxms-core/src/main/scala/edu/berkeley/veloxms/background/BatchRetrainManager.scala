package edu.berkeley.veloxms.background

import java.util.Date
import java.util.concurrent.TimeUnit

import dispatch.Defaults._
import dispatch._
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms.resources.internal.{HDFSLocation, LoadModelParameters}
import edu.berkeley.veloxms.util.EtcdClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class BatchRetrainManager[T](
    model: Model[T],
    hostPartitionMap: Seq[String],
    etcdClient: EtcdClient,
    sparkContext: SparkContext,
    sparkDataLocation: String,
    delay: Long,
    unit: TimeUnit)
  extends BackgroundTask(delay, unit) {

  override protected def execute(): Unit = batchTrain()
  private val http = Http.configure(_.setAllowPoolingConnection(true).setFollowRedirects(true))

  /**
   *  Server:
   *    - Acquire global retrain lock (lock per model) from etcd
   *    - Tell servers to disable online updates
   *        * Don't forget, this should be done for the retrain master as well
   *    - Tell servers to write obs to HDFS - servers respond to request when done
   *        * Don't forget, this should be done for the retrain master as well
   *    - Retrain in Spark
   *    - When Spark retrain done, request servers load new model - servers respond when done
   *        * Don't forget, this should be done for the retrain master as well
   *    - Tell servers to re-enable online updates
   *        * Don't forget, this should be done for the retrain master as well
   *    - Release retrain lock
   *
   */
  def batchTrain(): String = {
    val modelName = model.modelName
    var retrainResult = ""
    val veloxPort = 8080
    val hosts = hostPartitionMap.map{
      h => host(h, veloxPort).setContentType("application/json", "UTF-8")
    }

    // coordinate retraining: returns false if retrain already going on
    logInfo(s"Starting retrain for model $modelName")
    val lockAcquired = etcdClient.acquireRetrainLock(modelName)

    if (lockAcquired) {
      try {
        val nextVersion = new Date().getTime
        val obsDataLocation = HDFSLocation(s"$sparkDataLocation/$modelName/observations/$nextVersion")
        val newModelLocation = LoadModelParameters(s"$modelName/retrained_model/$nextVersion", nextVersion)

        // Disable online updates
        val disableOnlineUpdateRequests = hosts.map(
          h => {
            val req = (h / "disableonlineupdates" / modelName).POST
            http(req OK as.String)
          })

        val disableOnlineUpdateFutures = Future.sequence(disableOnlineUpdateRequests)
        val disableOnlineUpdateResponses = Await.result(disableOnlineUpdateFutures, Duration.Inf)
        logInfo(s"Disabled online updates: ${disableOnlineUpdateResponses.mkString("\n")}")

        // Write the observations to the spark server
        val writeRequests = hosts.map(
          h => {
            val req = (h / "writetrainingdata" / modelName)
                .POST << jsonMapper.writeValueAsString(obsDataLocation)
            http(req OK as.String)
          })

        val writeResponseFutures = Future.sequence(writeRequests)
        val writeResponses = Await.result(writeResponseFutures, Duration.Inf)
        logInfo(s"Write to spark cluster responses: ${writeResponses.mkString("\n")}")

        // Do the core batch retrain on spark
        val trainingData: RDD[(UserID, T, Double)] = sparkContext.objectFile(s"${obsDataLocation.loc}/*/*")

        val itemFeatures = model.retrainFeatureModelsInSpark(trainingData, nextVersion)
        val userWeights = model.retrainUserWeightsInSpark(itemFeatures, trainingData).map {
          case (userId, weights) => s"$userId, ${weights.mkString(", ")}"
        }

        userWeights.saveAsTextFile(s"$sparkDataLocation/${newModelLocation.userWeightsLoc}/users")
        logInfo("Finished retraining new model in spark")

        // Load the batch retrained models from spark & switch to the new models
        val loadModelRequests = hosts.map(
          h => {
            val req = (h / "loadmodel" / modelName)
                .POST << jsonMapper.writeValueAsString(newModelLocation)
            http(req OK as.String)
          })

        val loadResponseFutures = Future.sequence(loadModelRequests)
        val loadResponses = Await.result(loadResponseFutures, Duration.Inf)
        logInfo(s"Load new model responses: ${loadResponses.mkString("\n")}")

        retrainResult = "Success"
      } finally {
        // Re-enable the online updates
        val enableOnlineUpdateRequests = hosts.map(
          h => {
            val req = (h / "enableonlineupdates" / modelName).POST
            http(req OK as.String)
          })

        val enableOnlineUpdateFutures = Future.sequence(enableOnlineUpdateRequests)
        val enableOnlineUpdateResponses = Await.result(enableOnlineUpdateFutures, Duration.Inf)
        logInfo(s"re-enabled online updates: ${enableOnlineUpdateResponses.mkString("\n")}")

        // Release the training lock
        val lockReleased = etcdClient.releaseRetrainLock(modelName)
        logInfo(s"released lock successfully: $lockReleased")
      }
    } else {
      retrainResult = "Failed to acquire lock"
    }

    logInfo(s"Result of batch retrain: $retrainResult")
    retrainResult
  }
}
