package edu.berkeley.veloxms

import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.resources.internal._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.models._
import edu.berkeley.veloxms.util._
import io.dropwizard.Configuration
import io.dropwizard.Application
import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import com.fasterxml.jackson.annotation.JsonProperty
import javax.validation.constraints.NotNull
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.control.NonFatal

import edu.berkeley.veloxms.util._
import org.eclipse.jetty.servlet.ServletHolder

class VeloxConfiguration extends Configuration {
  val hostname: String = "none"
}

object VeloxEntry {

  final def main(args: Array[String]) {
    new VeloxApplication().run(args)
  }

}

class VeloxApplication extends Application[VeloxConfiguration] with Logging {

  val configEtcdPath = "cluster_config"

  override def getName = "velox model server"

  override def initialize(bootstrap: Bootstrap[VeloxConfiguration]) {
    bootstrap.getObjectMapper.registerModule(new DefaultScalaModule()) 
  }

  override def run(conf: VeloxConfiguration, env: Environment) {

    // Get cluster settings from etcd
    // FIXME this assumes that etcd is running on each velox server
    val etcdClient = new EtcdClient(conf.hostname, 4001, conf.hostname, new DispatchUtil)
    try {
      val partitionMap: Map[String, Int] = getPartitionMap(etcdClient)
      val sparkMaster = etcdClient.getValue(s"$configEtcdPath/sparkMaster")
      // Location for the spark cluster to write data to & from
      // Looks like hdfs://ec2-54-158-166-184.compute-1.amazonaws.com:9000/velox
      val sparkDataLocation: String = etcdClient.getValue(s"$configEtcdPath/sparkDataLocation")
      logInfo("Starting spark context")
      val sparkConf = new SparkConf()
          .setMaster(sparkMaster)
          .setAppName("VeloxOnSpark!")
          .setJars(SparkContext.jarOfObject(this).toSeq)

      val sparkContext = new SparkContext(sparkConf)
      val broadcastProvider = new SparkVersionedBroadcastProvider(sparkContext, sparkDataLocation)
      val prefix = s"$configEtcdPath/models"
      val modelNames = etcdClient.listDir(prefix).map(_.stripPrefix(s"/$prefix/"))

      logWarning(s"${modelNames.mkString(", ")}")

      val models = modelNames.map(name => createModel(name,
                                                      sparkContext,
                                                      etcdClient,
                                                      broadcastProvider,
                                                      sparkDataLocation,
                                                      partitionMap,
                                                      env,
                                                      conf.hostname))
      logInfo("Registered models: " + modelNames.mkString(", "))
      env.jersey().register(new ModelListResource(modelNames))
    } catch {
      case NonFatal(e) => {
        logError(e.getMessage())
        throw new VeloxInitializationException("Configuration error. Aborting Velox", e)
      }
      case e: InterruptedException => {
        logError(e.getMessage())
        throw new VeloxInitializationException("Configuration error. Aborting Velox", e)
      }
    }
    // env.jersey().register(new CacheHitResource(models.toMap))
  }

  def getPartitionMap(etcdClient: EtcdClient): Map[String, Int] = {
    val key = s"$configEtcdPath/veloxPartitions"
    val json = etcdClient.getValue(key)
    jsonMapper.readValue(json, classOf[Map[String, Int]])
  }


  def createModel(name: String,
                  sparkContext: SparkContext,
                  etcdClient: EtcdClient,
                  broadcastProvider: BroadcastProvider,
                  sparkDataLocation: String,
                  partitionMap: Map[String, Int],
                  env: Environment,
                  hostname: String
                  ): Model[_,_] = {

    val key = s"$configEtcdPath/models/$name"
    val json = etcdClient.getValue(key)
    val modelConfig = jsonMapper.readValue(json, classOf[VeloxModelConfig])
    val mc = modelConfig
    // TODO(crankshaw) cleanup model constructor code after Tomer
    // finishes refactoring model and storage configuration
    val averageUser = Array.fill[Double](mc.dimensions)(1.0)
    // TODO add newsgroups model loc
    val modelLoc = ""
    val model = modelConfig.modelType match {
      case "MatrixFactorizationModel" => new MatrixFactorizationModel(
          name,
          broadcastProvider,
          mc.dimensions,
          averageUser,
          mc.cachePartialSums,
          mc.cacheFeatures,
          mc.cachePredictions,
          0,
          partitionMap,
          etcdClient,
          sparkContext,
          sparkDataLocation)
      case "NewsgroupsModel" => new NewsgroupsModel(
          name,
          broadcastProvider,
          mc.modelLoc.get,
          mc.dimensions,
          averageUser,
          mc.cachePartialSums,
          mc.cacheFeatures,
          mc.cachePredictions,
          0,
          partitionMap,
          etcdClient,
          sparkContext,
          sparkDataLocation)
    }

    val predictServlet = new PointPredictionServlet(model, env.metrics().timer(name + "/predict/"))
    val topKServlet = new TopKPredictionServlet(model, env.metrics().timer(name + "/predict_top_k/"))
    val observeServlet = new AddObservationServlet(
        model,
        env.metrics().timer(name + "/observe/"))
    val writeHdfsServlet = new WriteToHDFSServlet(
        model,
        env.metrics().timer(name + "/observe/"),
        sparkContext,
        sparkDataLocation,
        partitionMap.get(hostname).getOrElse(-1))
    val retrainServlet = new RetrainServlet(
        model,
        env.metrics().timer(name + "/retrain/"),
        etcdClient,
        partitionMap)
    val loadNewModelServlet = new LoadNewModelServlet(
        model, 
        env.metrics().timer(name + "/loadmodel/"),
        sparkContext,
        sparkDataLocation)
    env.getApplicationContext.addServlet(new ServletHolder(predictServlet), "/predict/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(topKServlet), "/predict_top_k/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(observeServlet), "/observe/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(retrainServlet), "/retrain/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(writeHdfsServlet), "/writehdfs/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(loadNewModelServlet), "/loadmodel/" + name)
    model
  }
}

case class VeloxModelConfig(
  cacheFeatures: Boolean,
  cachePartialSums: Boolean,
  cachePredictions: Boolean,
  dimensions: Int,
  modelType: String,
  storageConfig: StorageConfig,
  modelLoc: Option[String]
)

case class StorageConfig(storageType: String)



