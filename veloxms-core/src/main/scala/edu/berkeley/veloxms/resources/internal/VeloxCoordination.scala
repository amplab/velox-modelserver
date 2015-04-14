package edu.berkeley.veloxms.resources.internal

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.Logging
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.commons.io.IOUtils
import java.io.StringWriter

import org.apache.spark.{SparkContext, SparkConf}

case class HDFSLocation(loc: String)
case class LoadModelParameters(userWeightsLoc: String, version: Version)

class WriteToHDFSServlet(model: Model[_, _], timer: Timer, sparkMaster: String, sparkDataLocation: String, partition: Int) extends HttpServlet
  with Logging {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {
      val obsLocation = jsonMapper.readValue(req.getInputStream, classOf[HDFSLocation])
      val uri = s"$sparkDataLocation/${obsLocation.loc}/part_$partition"

      val sparkHome = "/root/spark-1.3.0-bin-hadoop1"
      logWarning("Starting spark context")
      val sparkConf = new SparkConf()
          .setMaster(sparkMaster)
          .setAppName("VeloxOnSpark!")
          .setJars(SparkContext.jarOfObject(this).toSeq)
          .setSparkHome(sparkHome)
      // .set("spark.akka.logAkkaConfig", "true")
      val sc = new SparkContext(sparkConf)

      val observations = model.getObservationsAsRDD(sc)
      observations.saveAsObjectFile(uri)

      sc.stop()

      resp.setContentType("application/json");
      jsonMapper.writeValue(resp.getOutputStream, "success")
    } finally {
      timeContext.stop()
    }
  }

}


class LoadNewModelServlet(model: Model[_, _], timer: Timer, sparkMaster: String, sparkDataLocation: String)
    extends HttpServlet with Logging {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    val modelLocation = jsonMapper.readValue(req.getInputStream, classOf[LoadModelParameters])
    try {
      val uri = s"$sparkDataLocation/${modelLocation.userWeightsLoc}"

      val sparkHome = "/root/spark-1.3.0-bin-hadoop1"
      logWarning("Starting spark context")
      val sparkConf = new SparkConf()
          .setMaster(sparkMaster)
          .setAppName("VeloxOnSpark!")
          .setJars(SparkContext.jarOfObject(this).toSeq)
          .setSparkHome(sparkHome)

      val sc = new SparkContext(sparkConf)

      // TODO only add users in this partition: if (userId % partNum == 0)
      val users = sc.textFile(s"$uri/users/*").map(line => {
        val userSplits = line.split(", ")
        val userId = userSplits(0).toLong
        val userFeatures: Array[Double] = userSplits.drop(1).map(_.toDouble)
        // TODO this should be inserted into storage backend
        (userId, userFeatures)
      }).collect().toMap

      val firstUser = users.head
      logInfo(s"Loaded new models for ${users.size} users. " +
        s"First one is:\n${firstUser._1}, ${firstUser._2.mkString(", ")}")


      // TODO: Should make sure it's sufficiently atomic
      model.writeUserWeights(users, modelLocation.version)
      model.useVersion(modelLocation.version)

      resp.setContentType("application/json");
      jsonMapper.writeValue(resp.getOutputStream, "success")

    } finally {

      timeContext.stop()
    }
  }
}


