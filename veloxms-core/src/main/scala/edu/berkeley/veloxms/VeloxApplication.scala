package edu.berkeley.veloxms

import java.util.concurrent.TimeUnit

import edu.berkeley.veloxms.background.{OnlineUpdateManager, BatchRetrainManager}
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
import scala.reflect.ClassTag
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


  def registerModelResources[T : ClassTag](
      model: Model[T],
      name: String,
      sparkContext: SparkContext,
      etcdClient: EtcdClient,
      broadcastProvider: BroadcastProvider,
      sparkDataLocation: String,
      partitionMap: Map[String, Int],
      env: Environment,
      hostname: String): Unit = {
    val onlineUpdateManager = new OnlineUpdateManager(model, 5, TimeUnit.SECONDS, env.metrics().timer(s"$name/online_update"))
    // TODO (Dan): Have the env.lifecycle manage the batchretrainmanager w/ whatever delay settings you want
    val batchRetrainManager = new BatchRetrainManager(
      model,
      partitionMap,
      0,
      etcdClient,
      sparkContext,
      sparkDataLocation,
      50000,
      TimeUnit.SECONDS)

    val predictServlet = new PointPredictionServlet(model, env.metrics().timer(name + "/predict/"))
    val topKServlet = new TopKPredictionServlet(model, env.metrics().timer(name + "/predict_top_k/"))
    val enableOnlineUpdatesServlet = new EnableOnlineUpdates(
      onlineUpdateManager,
      env.metrics().timer(name + "/enableonlineupdates/"))
    val disableOnlineUpdatesServlet = new DisableOnlineUpdates(
      onlineUpdateManager,
      env.metrics().timer(name + "/disableonlineupdates/"))
    val observeServlet = new AddObservationServlet(
      onlineUpdateManager,
      env.metrics().timer(name + "/observe/"))
    val writeHdfsServlet = new WriteToHDFSServlet(
      model,
      env.metrics().timer(name + "/writehdfs/"),
      sparkContext,
      sparkDataLocation,
      partitionMap.getOrElse(hostname, -1))
    val retrainServlet = new RetrainServlet(
      batchRetrainManager,
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
    env.getApplicationContext.addServlet(new ServletHolder(enableOnlineUpdatesServlet), "/enableonlineupdates/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(disableOnlineUpdatesServlet), "/disableonlineupdates/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(writeHdfsServlet), "/writehdfs/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(loadNewModelServlet), "/loadmodel/" + name)
    env.lifecycle().manage(onlineUpdateManager)
  }

  def createModel(name: String,
                  sparkContext: SparkContext,
                  etcdClient: EtcdClient,
                  broadcastProvider: BroadcastProvider,
                  sparkDataLocation: String,
                  partitionMap: Map[String, Int],
                  env: Environment,
                  hostname: String
                  ): Unit = {

    val key = s"$configEtcdPath/models/$name"
    val json = etcdClient.getValue(key)
    val modelConfig = jsonMapper.readValue(json, classOf[VeloxModelConfig])

    // TODO(crankshaw) cleanup model constructor code after Tomer
    // finishes refactoring model and storage configuration
    val averageUser = Array.fill[Double](modelConfig.dimensions)(1.0)
    // TODO add newsgroups model loc
    val modelLoc = ""
    modelConfig.modelType match {
      case "MatrixFactorizationModel" =>
        val model = new MatrixFactorizationModel(
          name,
          broadcastProvider,
          modelConfig.dimensions,
          averageUser)
        registerModelResources(
          model,
          name,
          sparkContext: SparkContext,
          etcdClient: EtcdClient,
          broadcastProvider: BroadcastProvider,
          sparkDataLocation: String,
          partitionMap: Map[String, Int],
          env: Environment,
          hostname: String)
      case "NewsgroupsModel" =>
        val model = new NewsgroupsModel(
          name,
          broadcastProvider,
          modelConfig.modelLoc.get,
          modelConfig.dimensions,
          averageUser)
        registerModelResources(
          model,
          name,
          sparkContext: SparkContext,
          etcdClient: EtcdClient,
          broadcastProvider: BroadcastProvider,
          sparkDataLocation: String,
          partitionMap: Map[String, Int],
          env: Environment,
          hostname: String
        )
    }
  }
}

case class VeloxModelConfig(
  cacheFeatures: Boolean,
  cachePartialSums: Boolean,
  cachePredictions: Boolean,
  dimensions: Int,
  modelType: String,
  modelLoc: Option[String]
)




