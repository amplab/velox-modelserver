package edu.berkeley.veloxms.client

import com.fasterxml.jackson.databind.ObjectMapper
// import com.fasterxml.jackson.datatype.guava.GuavaModule
// import com.fasterxml.jackson.datatype.jdk7.Jdk7Module
import dispatch.as.json4s._
import com.ning.http.client.Response
import com.ning.http.client.extra.ThrottleRequestFilter
import java.util.concurrent.atomic.AtomicInteger
// import com.fasterxml.jackson.datatype.joda.JodaModule
// import com.fasterxml.jackson.module.afterburner.AfterburnerModule

import edu.berkeley.veloxms.util.Logging
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.JValue
import scala.collection.mutable
import scala.collection.immutable
import scala.io.Source
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

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
    veloxURLFile: String = null,
    veloxPort: Int = 8080,
    numRequests: Int = 10000,
    percentObs: Double = 0.2,
    numPartitions: Int = 4,
    numThreads: Int = 10,
    connTimeout: Int = 10000,
    throttleRequests: Int = 1000,
    statusTime: Int = 10

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
      opt[String]("veloxURLFile")
        // .required()
        .text(s"file containing mapping of urls to partitions")
        .action((x, c) => c.copy(veloxURLFile = x))
      opt[Int]("veloxPort")
        .text("port Velox server is listening to, " +
          s"default: ${defaultParams.veloxPort}")
        .action((x, c) => c.copy(veloxPort = x))
      opt[Int]("numRequests")
        .text("number of queries to run in this test " + 
          "(or if generating training data, the number of examples to create) " +
          s"default: ${defaultParams.numRequests}")
        .action((x, c) => c.copy(numRequests = x))
      opt[Double]("percentObs")
        .text("percent of requests that are observations, " +
          s"default: ${defaultParams.percentObs}")
        .action((x, c) => c.copy(percentObs = x))
      // opt[Unit]("genTrain")
      //   .text("Flag to generate training data rather than sending reqs")
      //   .action((_, c) => c.copy(genTrain = true))
      opt[Int]("numPartitions")
        .text("The number of partitions to divide training data into, " +
          s"default: ${defaultParams.numPartitions}")
        .action((x, c) => c.copy(numPartitions = x))
      opt[Int]("numThreads")
        .text("Number of threads to use to issue requests, " +
          s"default: ${defaultParams.numThreads}")
        .action((x, c) => c.copy(numThreads = x))
      opt[Int]("connTimeout")
        .text("TCP? connection timeout, " +
          s"default: ${defaultParams.connTimeout}")
        .action((x, c) => c.copy(connTimeout = x))
      opt[Int]("throttleRequests")
        .text("number of requests to send before throttling, " +
          s"default: ${defaultParams.throttleRequests}")
        .action((x, c) => c.copy(throttleRequests = x))
      opt[Int]("statusTime")
        .text("Time interval (s) to print out benchmark status, " +
          s"default: ${defaultParams.statusTime}")
        .action((x, c) => c.copy(statusTime = x))
      // opt[Int]("modelSize")
      //   .text("The number of factors in the model, " +
      //     s"default: ${defaultParams.modelSize}")
      //   .action((x, c) => c.copy(modelSize = x))



      note(
        """
          TODO: sample instructions
        """.stripMargin)
    }

    Logger.getRootLogger.setLevel(Level.INFO)
    parser.parse(args, defaultParams).map { params =>
      // if (params.genTrain) {
      //   val requestor = new MFRequestor(
      //     numUsers = params.numUsers,
      //     numItems = params.numItems,
      //     percentObs = 1.0) // all requests should be observations
      //   generateTrainingData(requestor, params)
      // } else {
      val requestor = new MFRequestor(
        numUsers = params.numUsers,
        numItems = params.numItems,
        percentObs = params.percentObs)
      sendRequests(requestor, params)
    } getOrElse {
      sys.exit(1)
    }

  }

  /**
   * This will randomly generate base-line item models for each item and user-models for each
   * user, as well as random (uncorrelated) training data for each user, so that the online
   * updates will have some data to work with.
   * The item models will not be split, but the user and training data will be partitioned
   * by user into the number of partitions specified in params.
   */
  // def generateTrainingData(requestor: MFRequestor, params: Params) {
  //   // generate item models:
  //   
  //   val rand = new Random
  //
  //   var i = 0L
  //   val itemMap = new mutable.HashMap[Long, Array[Double]]
  //   while (i < params.numItems) {
  //     itemMap.put(i, randomArray(rand, params.modelSize))
  //     ++i
  //   }
  //
  //   val userMap = new mutable.HashMap[Long, Array[Double]]
  //   while (i < params.numItems) {
  //     userMap.put(i, randomArray(rand, params.modelSize))
  //     ++i
  //   }
  //
  //   val sizeOfPart = params.numUsers / params.numPartitions
  //   val partitionedUserMap = userMap.groupBy {(k, v) =>
  //     k / sizeOfPart
  //   }
  //   val createdNumPartitions = partitionedUserMap.size
  //   if (createdNumPartitions != params.numPartitions) {
  //     logWarning(s"Intended to generate ${params.numPartitions} partitions, ended up" + 
  //       s" with $createdNumPartitions")
  //   }
  //
  //   // val trainingData = new mutable.HashMap[Long
  //
  //
  //
  //
  //
  //
  //
  // }

  def getHosts(hostsFile: String, port: Int): Map[Int, Req] = {
    val hosts = Source.fromFile(hostsFile)
      .getLines
      .map( line => {
        val splits = line.split(": ")
        val part = splits(0).toInt
        val url = splits(1)
        (part, host(url, port).setContentType("application/json", "UTF-8"))
      }).toMap
      hosts
  }

  def sendRequests(requestor: MFRequestor, params: Params) {
    val http = Http.configure(_.setAllowPoolingConnection(true)
      .setConnectionTimeoutInMs(params.connTimeout)
      // .setMaximumConnectionsTotal(1000)
      .addRequestFilter(new ThrottleRequestFilter(params.throttleRequests))
    )

      // .setMaximumConnectionsPerHost(1000)
      // .setRequestTimeoutInMs(1000)
    // .setExecutorService(pool)

// builder => {
//     builder.setConnectionTimeoutInMs(1000)
//     builder.setAllowPoolingConnection(true)
//     builder.setMaximumConnectionsTotal(1000)
//     builder.addRequestFilter(new ThrottleRequestFilter(1));
//     builder.build()
//     builder
    // val parallelism = params.numThreads



    val nanospersec = math.pow(10, 9)


    var numPreds = 0
    var numObs = 0

    
    val reqPerPartition = new Array[Int](params.numPartitions)

    // val veloxHost = host(params.veloxURL, params.veloxPort)
    //       .setContentType("application/json", "UTF-8")
    val hosts = getHosts(params.veloxURLFile, params.veloxPort)
    val observePath = "observe"
    val predictPath = "predict"
    val modelPath = "matrixfact"
    //json encoding
    val mapper = createObjectMapper

    def createPredictRequest(req: PredictRequest): Req = {
      val jsonString = mapper.writeValueAsString(req)
      // val jsonString = mapper.writeValueAsString(PredictRequest(1, 4))
      val partition = (req.user % params.numPartitions).toInt
      reqPerPartition(partition) += 1
      val veloxHost = hosts(partition)
      val request = (veloxHost / predictPath / modelPath).POST << jsonString
      numPreds += 1
      request
    }

    def createObserveRequest(req: ObserveRequest): Req = {
      val partition = (req.userId % params.numPartitions).toInt
      reqPerPartition(partition) += 1
      val veloxHost = hosts(partition)
      val jsonString = mapper.writeValueAsString(req)
      val request = (veloxHost / observePath / modelPath).POST << jsonString
      numObs += 1
      request
    }

    val startTime = System.nanoTime


    // TODO: Figure out how to compute latency
    // val responseFutures: IndexedSeq[Try[(Long, JValue)]] =
    //   for (i <- 0 to params.numRequests)
    //     yield requestor.getRequest match {
    //       case Left(oreq) => {
    //         val reqStart = System.nanoTime
    //         val f = http(createObserveRequest(oreq) OK as.json4s.Json)
    //         f onComplete {
    //           case Success(jv) => {
    //             val reqStop = System.nanoTime
    //             Success((reqStop - reqStart, jv))
    //           }
    //           case Failure(t) => {
    //             logWarning(s"An error has occurred: ${t.getMessage()}")
    //             Failure(t)
    //           }
    //         }
    //       }
    //       case Right(preq) => {
    //         val reqStart = System.nanoTime
    //         val f = http(createPredictRequest(preq) OK as.json4s.Json)
    //         f onComplete {
    //           case Success(jv) => {
    //             val reqStop = System.nanoTime
    //             Success((reqStop - reqStart, jv))
    //           }
    //           case Failure(t) => {
    //             logWarning(s"An error has occurred: ${t.getMessage()}")
    //             Failure(t)
    //           }
    //         }
    //       }
    //     }




    val responseFutures =
      for (i <- 0 until params.numRequests)
        yield requestor.getRequest.fold(
          oreq => http(createObserveRequest(oreq) OK as.json4s.Json).either,
          preq => http(createPredictRequest(preq) OK as.json4s.Json).either
        )
    val responses: Future[IndexedSeq[Either[Throwable, JValue]]] = Future.sequence(responseFutures)

    val (lefts, rights) = responses().partition(_.isInstanceOf[Left[_,_]])

    val failures = for(e <- lefts)
      yield for (l <- e.left)
        yield l.toString

    val successes = for(e <- rights)
      yield for (r <- e.right)
        yield r.toString

    val stopTime = System.nanoTime
    val elapsedTime = (stopTime - startTime) / nanospersec
    val pthruput = numPreds.toDouble / elapsedTime.toDouble
    val othruput = numObs.toDouble / elapsedTime.toDouble
    val totthruput = (successes.size).toDouble / elapsedTime.toDouble
    logInfo(s"In $elapsedTime seconds " +
            s"sent $numPreds predictions ($pthruput ops/sec), " +
            s" $numObs observations ($othruput ops/sec), " +
            s"\n${successes.size} successes and ${failures.size} failures" +
            s"\nTOTAL SUCCESSFUL THROUGHPUT: $totthruput ops/sec")

    http.shutdown()
    System.exit(0)

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







