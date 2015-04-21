package edu.berkeley.veloxms.models

import javax.validation.constraints.NotNull

import com.fasterxml.jackson.annotation.JsonProperty
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage.BroadcastProvider
import edu.berkeley.veloxms.util.{EtcdClient, Logging}
import io.dropwizard.setup.Environment
import org.hibernate.validator.constraints.NotEmpty
import scala.io.Source

/**
 * A storage factory to provide the configured model storage
 */
class ModelFactory extends Logging {
  @NotEmpty val modelType: String = ""
  @NotNull val modelSize: Integer = 50
  val partitionFile: String = ""
  val modelLoc: String = ""
  val cachePartialSums: Boolean = false
  val cachePredictions: Boolean = false
  val cacheFeatures: Boolean = false

  def build(env: Environment, modelName: String, hostname: String, broadcastProvider: BroadcastProvider): (Model[_, _], Int, Map[String, Int]) = {
    modelType match {
      case "MatrixFactorizationModel" => {
        require(partitionFile != "")
        val (partition, partitionMap) = getPartition(partitionFile, hostname)
        require(partition > -1)
        
        val averageUser = Array.fill[Double](modelSize)(1.0)
        val model = new MatrixFactorizationModel(
            modelName,
            broadcastProvider,
            modelSize,
            averageUser,
            cachePartialSums,
            cacheFeatures,
            cachePredictions
          )

        (model, partition, partitionMap)

      }
      // To use the newsgroups model, the binary file containing the model must
      // be specified in modelLoc. The binary is not included in the repo, but can be
      // downloaded from https://s3.amazonaws.com/velox-public/news-classifier-from-tomer
      case "NewsgroupsModel" => {
        require(partitionFile != "")
        require(modelLoc != "")
        val (partition, partitionMap) = getPartition(partitionFile, hostname)
        require(partition > -1)

        val averageUser = Array.fill[Double](modelSize)(1.0)
        val model = new NewsgroupsModel(
            modelName,
            broadcastProvider,
            modelLoc,
            modelSize,
            averageUser,
            cachePartialSums,
            cacheFeatures,
            cachePredictions
          )

        (model, partition, partitionMap)
      }
      case _ => {
        throw new IllegalArgumentException(s"Unknown model type: $modelType")
      }
    }
  }

  def getPartition(partitionFile: String, hostname: String): (Int, Map[String, Int]) = {
    logInfo(s"My host name: $hostname")
    val hosts = Source.fromFile(partitionFile)
      .getLines()
      .map( line => {
        val splits = line.split(": ")
        val url = splits(0)
        val part = splits(1).toInt
        logInfo(line)
        (url, part)
      }).toMap
    val partition = hosts.getOrElse(hostname, -1)
    (partition, hosts)
  }
}
