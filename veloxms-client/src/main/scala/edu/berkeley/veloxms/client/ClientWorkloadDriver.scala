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
      request
    }

    def createObserveRequest(req: ObserveRequest): Req = {
      val partition = (req.userId % params.numPartitions).toInt
      reqPerPartition(partition) += 1
      val veloxHost = hosts(partition)
      val jsonString = mapper.writeValueAsString(req)
      val request = (veloxHost / observePath / modelPath).POST << jsonString
      request
    }


    // val req = Http(createObserveRequest(ObserveRequest(22, 51, 1.3)) OK as.json4s.Json).either
    // req().fold(
    //   (err) => println(s"ERROR: $err"),
    //   (good) => println(s"Request result: $good")
    // )
    // System.exit(0)

    val numPreds = new AtomicInteger(0)
    val numObs = new AtomicInteger(0)
    val numFailed = new AtomicInteger(0)

    val opsSent = new AtomicInteger(0)
    val opsDone = new AtomicInteger(0)

    val nanospersec = math.pow(10, 9)
    val numops = params.numRequests

    def opDone() {
      val o = opsDone.incrementAndGet
      if (o == numops) {
        opsDone.synchronized {
          opsDone.notify()
        }
      }
    }
    
    val startTime = System.nanoTime

    // val responseFutures =
    for (i <- 0 to params.numThreads) {
      new Thread(new Runnable {
        override def run() = {
          while (opsSent.get() < numops) {
            requestor.getRequest match {
              case Left(oreq) => {
                val f = http(createObserveRequest(oreq) OK as.json4s.Json)
                f onComplete {
                  case Success(jv) => {
                    numObs.incrementAndGet()
                    opDone
                  }
                  case Failure(t) => {
                    logWarning(s"An error has occurred: ${t.getMessage()}")
                    numFailed.incrementAndGet()
                  }
                }
              }
              case Right(preq) => {
                val f = http(createPredictRequest(preq) OK as.json4s.Json)
                f onComplete {
                  case Success(jv) => {
                    numPreds.incrementAndGet()
                    opDone
                  }
                  case Failure(t) => {
                    logWarning(s"An error has occurred: ${t.getMessage()}")
                    numFailed.incrementAndGet()
                  }
                }
              }
            }
            opsSent.incrementAndGet()
          }
          logWarning(s"Thread $i is done sending")
        }
      }).start()
    }


      new Thread(new Runnable {
        override def run() {
          while(opsDone.get() < numops) {
            Thread.sleep(params.statusTime*1000)
            val curTime = (System.nanoTime-startTime).toDouble/nanospersec
            val curThru = (numPreds.get()+numObs.get()).toDouble/curTime
            logInfo(s"STATUS @ ${curTime}s: $curThru ops/sec ($opsDone ops done)")
          }
        }
      }).start

    val waitTimeSeconds = 200
    opsDone.synchronized {
      opsDone.wait(waitTimeSeconds * 1000)
    }

    val stopTime = System.nanoTime
    val elapsedTime = (stopTime - startTime) / nanospersec
    val npreds = numPreds.get()
    val nobs = numObs.get()
    val pthruput = npreds.toDouble / elapsedTime.toDouble
    val othruput = nobs.toDouble / elapsedTime.toDouble
    val totthruput = (npreds + nobs).toDouble / elapsedTime.toDouble
    logInfo(s"In $elapsedTime seconds with ${params.numThreads} threads, " +
            s"completed $npreds predictions ($pthruput ops/sec), " +
            s" $nobs observations ($othruput ops/sec), " +
            s"${numFailed.get()} failures\nTOTAL THROUGHPUT: $totthruput ops/sec")

    http.shutdown()
    System.exit(0)

    // val responseFutures =
    //   for (i <- 0 until params.numRequests)
    //     yield requestor.getRequest.fold(
    //       oreq => http(createObserveRequest(oreq) OK as.json4s.Json).either,
    //       preq => http(createPredictRequest(preq) OK as.json4s.Json).either
    //     )
    // val responses: Future[IndexedSeq[Either[Throwable, JValue]]] = Future.sequence(responseFutures)
    //
    // val (lefts, rights) = responses().partition(_.isInstanceOf[Left[_,_]])
    //
    // val failures = for(e <- lefts)
    //   yield for (l <- e.left)
    //     yield l.toString
    //
    // val successes = for(e <- rights)
    //   yield for (r <- e.right)
    //     yield r.toString



    // logInfo(s"${failures.size} requests FAILED, ${successes.size} requests SUCCEEDED")
    // println(s"${failures.size} requests FAILED, ${successes.size} requests SUCCEEDED")
    // println(s"Distribution of requests:")
    // for(i <- (0 until reqPerPartition.size)) {
    //   println(s"partition $i: ${reqPerPartition(i)}")
    // }

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







