package edu.berkeley.veloxms

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.berkeley.veloxms.background.{BatchRetrainManager, OnlineUpdateManager}
import edu.berkeley.veloxms.models._
import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.resources.internal._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.util._
import io.dropwizard.{Application, Configuration}
import io.dropwizard.setup.{Bootstrap, Environment}
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.jetty.servlet.ServletHolder

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.reflect.runtime.{universe => ru}
import com.fasterxml.jackson.databind.JsonNode

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
    val jsonTree = jsonMapper.readTree(json)
    require(jsonTree.has("modelType"))
    require(jsonTree.has("onlineUpdateDelayInMillis"))
    require(jsonTree.has("batchRetrainDelayInMillis"))
    val modelType = jsonTree.get("modelType").asText
    val onlineUpdateDelay = jsonTree.get("onlineUpdateDelayInMillis").asLong
    val batchRetrainDelay = jsonTree.get("batchRetrainDelayInMillis").asLong
    val modelSpecificConfig: Option[JsonNode] = if (jsonTree.has("config")) {
      Some(jsonTree.get("config"))
    } else {
      None
    }
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val clz = Class.forName(modelType)
    val classSymbol = mirror.classSymbol(clz)
    val cm = mirror.reflectClass(classSymbol)
    val constructor = classSymbol.toType.declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(constructor)
    val model = ctorm(name, broadcastProvider, modelSpecificConfig).asInstanceOf[Model[_]]
    registerModelResources(
      model,
      name,
      onlineUpdateDelay,
      batchRetrainDelay,
      sparkContext,
      etcdClient,
      broadcastProvider,
      sparkDataLocation,
      partitionMap,
      env,
      hostname)
  }

  def registerModelResources(
      model: Model[_],
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
    val downloadObservationsServlet = new DownloadBulkObservationsServlet(
      onlineUpdateManager,
      env.metrics().timer(name + "/downloadobservations/"),
      name,
      partitionMap,
      hostname,
      sparkContext)
    val saveObservationsServlet = new SaveObservationsServlet(
      env.metrics().timer(name + "/saveobservations/"),
      name,
      partitionMap,
      hostname)
    val loadObservationsServlet = new LoadObservationsServlet(
      env.metrics().timer(name + "/loadobservations/"),
      name,
      partitionMap,
      hostname)

    env.getApplicationContext.addServlet(new ServletHolder(predictServlet), "/predict/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(topKServlet), "/predict_top_k/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(observeServlet), "/observe/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(retrainServlet), "/retrain/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(enableOnlineUpdatesServlet), "/enableonlineupdates/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(disableOnlineUpdatesServlet), "/disableonlineupdates/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(writeTrainingDataServlet), "/writetrainingdata/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(loadNewModelServlet), "/loadmodel/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(downloadObservationsServlet), "/downloadobservations/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(saveObservationsServlet), "/saveobservations/" + name)
    env.getApplicationContext.addServlet(new ServletHolder(loadObservationsServlet), "/loadobservations/" + name)
  }

}

