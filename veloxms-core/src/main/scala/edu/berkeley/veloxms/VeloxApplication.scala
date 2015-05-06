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
      val partitionMap: Seq[String] = getPartitionMap(etcdClient)
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

      val models = modelNames.foreach(name => createModel(name,
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

  def getPartitionMap(etcdClient: EtcdClient): Seq[String] = {
    val key = s"$configEtcdPath/veloxPartitions"
    val json = etcdClient.getValue(key)
    jsonMapper.readValue(json, classOf[Seq[String]])
  }


  def registerModelResources[T : ClassTag](
      model: Model[T],
      name: String,
      onlineUpdateDelayInMillis: Long,
      batchRetrainDelayInMillis: Long,
      sparkContext: SparkContext,
      etcdClient: EtcdClient,
      broadcastProvider: BroadcastProvider,
      sparkDataLocation: String,
      partitionMap: Seq[String],
      env: Environment,
      hostname: String): Unit = {
    val masterPartition = Utils.nonNegativeMod(name.hashCode, partitionMap.size)
    val onlineUpdateManager = new OnlineUpdateManager(model, onlineUpdateDelayInMillis, TimeUnit.MILLISECONDS, env.metrics().timer(s"$name/online_update"))
    val batchRetrainManager = new BatchRetrainManager(
      model,
      partitionMap,
      etcdClient,
      sparkContext,
      sparkDataLocation,
      batchRetrainDelayInMillis,
      TimeUnit.MILLISECONDS)

    env.lifecycle().manage(onlineUpdateManager)
    // If this is the master partition, schedule background bulk retrains
    if (hostname == partitionMap(masterPartition)) {
      env.lifecycle().manage(batchRetrainManager)
    }

    val predictServlet = new PointPredictionServlet(
      model,
      env.metrics().timer(name + "/predict/"),
      partitionMap,
      hostname)
    val topKServlet = new TopKPredictionServlet(
      model,
      env.metrics().timer(name + "/predict_top_k/"),
      partitionMap,
      hostname)
    val enableOnlineUpdatesServlet = new EnableOnlineUpdates(
      onlineUpdateManager,
      env.metrics().timer(name + "/enableonlineupdates/"))
    val disableOnlineUpdatesServlet = new DisableOnlineUpdates(
      onlineUpdateManager,
      env.metrics().timer(name + "/disableonlineupdates/"))
    val observeServlet = new AddObservationServlet(
      onlineUpdateManager,
      env.metrics().timer(name + "/observe/"),
      name,
      partitionMap,
      hostname)
    val writeTrainingDataServlet = new WriteTrainingDataServlet(
      model,
      env.metrics().timer(name + "/writetrainingdata/"),
      sparkContext,
      sparkDataLocation,
      partitionMap.indexOf(hostname))
    val retrainServlet = new RetrainServlet(
      batchRetrainManager,
      env.metrics().timer(name + "/retrain/"),
      etcdClient)
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
    env.getApplicationContext.addServlet(new ServletHolder(writeTrainingDataServlet), "/writetrainingdata/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(loadNewModelServlet), "/loadmodel/" + name)
  }

  def createModel(name: String,
                  sparkContext: SparkContext,
                  etcdClient: EtcdClient,
                  broadcastProvider: BroadcastProvider,
                  sparkDataLocation: String,
                  partitionMap: Seq[String],
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
          modelConfig.onlineUpdateDelayInMillis,
          modelConfig.batchRetrainDelayInMillis,
          sparkContext,
          etcdClient,
          broadcastProvider,
          sparkDataLocation,
          partitionMap,
          env,
          hostname)
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
          modelConfig.onlineUpdateDelayInMillis,
          modelConfig.batchRetrainDelayInMillis,
          sparkContext,
          etcdClient,
          broadcastProvider,
          sparkDataLocation,
          partitionMap,
          env,
          hostname)
    }
  }
}

case class VeloxModelConfig(
  dimensions: Int,
  modelType: String,
  modelLoc: Option[String],
  onlineUpdateDelayInMillis: Long,
  batchRetrainDelayInMillis: Long
)




