package edu.berkeley.veloxms

import edu.berkeley.veloxms.models.ModelFactory
import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.storage._
import io.dropwizard.Configuration

import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import com.fasterxml.jackson.annotation.JsonProperty
import com.massrelevance.dropwizard.ScalaApplication
import com.massrelevance.dropwizard.bundles.ScalaBundle
import javax.validation.constraints.NotNull

import edu.berkeley.veloxms.util.Logging
import org.eclipse.jetty.servlet.ServletHolder

class VeloxConfiguration extends Configuration {
  val sparkMaster: String = "NoSparkMaster"

  @(JsonProperty)("models")
  val modelFactories: Map[String, ModelFactory] = Map()
  // sparkMaster: String
  // whether to do preprocessing of dataset for testing purposes
  // reloadTachyon: Boolean,
  // rawDataLoc: String
}


object VeloxApplication extends ScalaApplication[VeloxConfiguration] with Logging {

  override def getName = "velox model server"

  override def initialize(bootstrap: Bootstrap[VeloxConfiguration]) {
    bootstrap.addBundle(new ScalaBundle)
    // init global state
  }

  override def run(conf: VeloxConfiguration, env: Environment) {
    conf.modelFactories.foreach { case (name, modelFactory) => {
      val model = modelFactory.build(env)
      val predictServlet = new PointPredictionServlet(model, env.metrics().timer(name + "/predict/"))
      val observeServlet = new AddObservationServlet(model, conf.sparkMaster, env.metrics().timer(name + "/observe/"))
      val retrainServlet = new RetrainServlet(model, conf.sparkMaster, env.metrics().timer(name + "/retrain/"))
      env.getApplicationContext.addServlet(new ServletHolder(predictServlet), "/predict/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(observeServlet), "/observe/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(retrainServlet), "/retrain/" + name)
    }}
    logInfo("Registered models: " + conf.modelFactories.keys.mkString(","))
    env.jersey().register(new ModelListResource(conf.modelFactories.keys.toSeq))
  }
}


