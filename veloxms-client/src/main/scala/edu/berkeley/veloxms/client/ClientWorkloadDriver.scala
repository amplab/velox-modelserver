package edu.berkeley.veloxms.client

import com.fasterxml.jackson.databind.ObjectMapper
// import com.fasterxml.jackson.datatype.guava.GuavaModule
// import com.fasterxml.jackson.datatype.jdk7.Jdk7Module
import dispatch.as.json4s._
import com.ning.http.client.Response
// import com.fasterxml.jackson.datatype.joda.JodaModule
// import com.fasterxml.jackson.module.afterburner.AfterburnerModule

import edu.berkeley.veloxms.util.Logging
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.JValue

object ModelType extends Enumeration {
  type ModelType = Value
  val MatrixFactor, DecisionTree = Value
}

import ModelType._

import dispatch._, Defaults._
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

object VeloxWorkloadDriver extends Logging {


  case class Params(
    numUsers: Int = 100,
    numItems: Int = 100,
    modelType: ModelType = MatrixFactor,
    veloxURL: String = null,
    veloxPort: Int = 8080,
    numRequests: Int = 10000,
    percentObs: Double = 0.2
  )


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params](s"Velox client driver") {
      head("Generate synthetic workload for Velox")
      opt[Int]("numUsers")
        .text("number of different users")
        .action((x, c) => c.copy(numUsers = x))
      opt[Int]("numItems")
        .text("number of different items")
        .action((x, c) => c.copy(numItems = x))
      opt[String]("model")
        .text(s"model (${ModelType.values.mkString(",")}), " +
        s"default: ${defaultParams.modelType}")
        .action((x, c) => c.copy(modelType = ModelType.withName(x)))
      opt[String]("veloxURL")
        .required()
        .text(s"url of Velox server")
        .action((x, c) => c.copy(veloxURL = x))
      opt[Int]("veloxPort")
        .text("port Velox server is listening to, " +
          s"default: ${defaultParams.veloxPort}")
        .action((x, c) => c.copy(veloxPort = x))
      opt[Int]("numRequests")
        .text("number of queries to run in this test, " +
          s"default: ${defaultParams.numRequests}")
        .action((x, c) => c.copy(numRequests = x))
      opt[Double]("percentObs")
        .text("percent of requests that are observations, " +
          s"default: ${defaultParams.percentObs}")
        .action((x, c) => c.copy(percentObs = x))

      note(
        """
          TODO: sample instructions
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

  }

  def run(params: Params) {

    Logger.getRootLogger.setLevel(Level.INFO)
    val http = Http

    val veloxHost = host(params.veloxURL, params.veloxPort)
          .setContentType("application/json", "UTF-8")
    val observePath = "observe"
    val predictPath = "predict"
    val modelPath = "matrixfact"
    val mapper = createObjectMapper

    def createPredictRequest(req: PredictRequest): Req = {
      val jsonString = mapper.writeValueAsString(req)
      // val jsonString = mapper.writeValueAsString(PredictRequest(1, 4))
      val request = (veloxHost / predictPath / modelPath).POST << jsonString
      request
    }

    def createObserveRequest(req: ObserveRequest): Req = {
      val jsonString = mapper.writeValueAsString(req)
      val request = (veloxHost / observePath / modelPath).POST << jsonString
      request
    }

    val requestor = new MFRequestor(
      numUsers = params.numUsers,
      numItems = params.numItems,
      percentObs = params.percentObs)

    // val req = Http(createObserveRequest(ObserveRequest(22, 51, 1.3)) OK as.json4s.Json).either
    // req().fold(
    //   (err) => println(s"ERROR: $err"),
    //   (good) => println(s"Request result: $good")
    // )
    // System.exit(0)


    val responseFutures =
      for (i <- 0 until params.numRequests)
        yield requestor.getRequest.fold(
          (oreq) => http(createObserveRequest(oreq) OK as.json4s.Json).either,
          (preq) => http(createPredictRequest(preq) OK as.json4s.Json).either
        )
    val responses: Future[IndexedSeq[Either[Throwable, JValue]]] = Future.sequence(responseFutures)

    val (lefts, rights) = responses().partition(_.isInstanceOf[Left[_,_]])

    val failures = for(e <- lefts)
      yield for (l <- e.left)
        yield l.toString
    
    val successes = for(e <- rights)
      yield for (r <- e.right)
        yield r.toString

    logInfo(s"${failures.size} requests FAILED, ${successes.size} requests SUCCEEDED")
    println(s"${failures.size} requests FAILED, ${successes.size} requests SUCCEEDED")
    http.shutdown()

    // countResponses()
    //
    // System.exit(0)
  }



  def createObjectMapper: ObjectMapper = {
    val mapper = new ObjectMapper
    mapper.registerModule(new DefaultScalaModule)
    // mapper.registerModule(new GuavaExtrasModule)
    // mapper.registerModule(new Jdk7Module)
    // mapper.setPropertyNamingStrategy(new AnnotationSensitivePropertyNamingStrategy)
    // mapper.setSubtypeResolver(new DiscoverableSubtypeResolver)
  }

}







