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

import edu.berkeley.veloxms.util.{Logging, NGramDocumentGenerator}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.JValue
import scala.collection.mutable
import scala.collection.immutable
import scala.io.Source
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.xml.pull._
import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

object ModelType extends Enumeration {
  type ModelType = Value
  val MatrixFactorizationModel, NewsgroupsModel = Value
}

import ModelType._

import dispatch._, Defaults._
import scopt.OptionParser
// import org.apache.log4j.{Level, Logger}

object VeloxWorkloadDriver extends Logging {


  case class Params(
    numUsers: Int = 100,
    numItems: Int = 100,
    modelType: ModelType = MatrixFactorizationModel,
    veloxURLFile: String = null,
    veloxPort: Int = 8080,
    numRequests: Int = 10000,
    percentObs: Double = 0.2,
    percentTopK: Double = 0.1,
    numPartitions: Int = 4,
    numThreads: Int = 10,
    connTimeout: Int = 10000,
    throttleRequests: Int = 1000,
    statusTime: Int = 10,
    ngramFile: String = null,
    docLength: Int = 200
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
      opt[Double]("percentTopK")
        .text("percent of requests that are top-k, " +
          s"default: ${defaultParams.percentTopK}")
        .action((x, c) => c.copy(percentTopK = x))
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
      opt[String]("ngramFile")
        // .required()
        .text(s"File containing all word 1,2,3-grams in 20newsgroups. Used to generate " +
          "random documents")
        .action((x, c) => c.copy(ngramFile = x))
      opt[Int]("docLength")
        .text("Number of ngrams in documents in corpus, " +
          s"default: ${defaultParams.docLength}")
        .action((x, c) => c.copy(docLength = x))
      // opt[Int]("modelSize")
      //   .text("The number of factors in the model, " +
      //     s"default: ${defaultParams.modelSize}")
      //   .action((x, c) => c.copy(modelSize = x))



      note(
        """
          TODO: sample instructions
        """.stripMargin)
    }

    // Logger.getRootLogger.setLevel(Level.INFO)
    parser.parse(args, defaultParams).map { params =>
      // if (params.genTrain) {
      //   val requestor = new MFRequestor(
      //     numUsers = params.numUsers,
      //     numItems = params.numItems,
      //     percentObs = 1.0) // all requests should be observations
      //   generateTrainingData(requestor, params)
      // } else {
      val corpus = params.modelType match {
        case NewsgroupsModel => Some(
          NGramDocumentGenerator.createCorpus(params.numItems,
                                              params.docLength,
                                              params.ngramFile))
        case _ => None
      }

      val requestor = new Requestor(
        numUsers = params.numUsers,
        numItems = params.numItems,
        percentObs = params.percentObs)
      sendRequests(requestor, params, corpus)
    } getOrElse {
      sys.exit(1)
    }
  }

  def parsePlainText(textFile: String): Array[String] = {
    val paragraphSep = "xxxxxxxxxxxxxxx"
    val text = Source.fromFile(textFile)
      .mkString("")
      .split(paragraphSep)
      .map(_.replaceAll("[^a-zA-Z0-9]", ""))
    var totalSize = 0.0
    text.foreach {s => totalSize += s.size }
    println(s"Average doc size: ${totalSize / text.size.toDouble} for ${text.size} docs")
    text
  }


  def getHosts(hostsFile: String, port: Int): Map[Int, Req] = {
    val hosts = Source.fromFile(hostsFile)
      .getLines
      .map( line => {
        val splits = line.split(": ")
        val url = splits(0)
        val part = splits(1).toInt
        (part, host(url, port).setContentType("application/json", "UTF-8"))
      }).toMap
      hosts
  }

  def sendRequests(requestor: Requestor,
    params: Params,
    corpus: Option[Array[String]] = None ) {
    val http = Http.configure(_.setAllowPoolingConnection(true)
      .setConnectionTimeoutInMs(params.connTimeout)
      // .setMaximumConnectionsTotal(1000)
      .addRequestFilter(new ThrottleRequestFilter(params.throttleRequests))
    )

    val nanospersec = math.pow(10, 9)
    var numPreds = 0
    var numTopK = 0
    var numObs = 0
    val reqPerPartition = new Array[Int](params.numPartitions)
    val hosts = getHosts(params.veloxURLFile, params.veloxPort)
    val observePath = "observe"
    val retrainPath = "retrain"
    val predictPath = "predict"
    val topKPath = "predict_top_k"
    val modelPath = params.modelType match {
      case NewsgroupsModel => "newsgroups"
      case _ => "matrixfact"
    }
    //json encoding
    val mapper = createObjectMapper

    def createPredictRequest(req: PredictRequest): Req = {
      val jsonString = params.modelType match {
        case NewsgroupsModel => {
          val newsReq = NewsPredictRequest(req.uid, (corpus.get)(req.context.toInt))
          mapper.writeValueAsString(newsReq)
        }
        case _ => mapper.writeValueAsString(req)
      }
      // val jsonString = mapper.writeValueAsString(PredictRequest(1, 4))
      val partition = (req.uid % params.numPartitions).toInt
      reqPerPartition(partition) += 1
      val veloxHost = hosts(partition)
      val request = (veloxHost / predictPath / modelPath).POST << jsonString
      numPreds += 1
      request
    }

    def createTopKRequest(req: TopKPredictRequest): Req = {
      val jsonString: String = params.modelType match {
        case NewsgroupsModel => {
          ""
        }

        case _ => mapper.writeValueAsString(req)
      }

      val partition = (req.uid % params.numPartitions).toInt
      reqPerPartition(partition) += 1
      val veloxHost = hosts(partition)
      val request = (veloxHost / topKPath / modelPath).POST << jsonString
      numTopK += 1
      request
    }

    def createObserveRequest(req: ObserveRequest): Req = {
      val partition = (req.uid % params.numPartitions).toInt
      reqPerPartition(partition) += 1
      val veloxHost = hosts(partition)
      val jsonString = params.modelType match {
        case NewsgroupsModel => {
          val newsReq = NewsObserveRequest(req.uid, (corpus.get)(req.context.toInt), req.score)
          mapper.writeValueAsString(newsReq)
        }
        case _ => mapper.writeValueAsString(req)
      }
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
          l => l.fold(
            oreq => http(createObserveRequest(oreq) OK as.json4s.Json),
            preq => http(createPredictRequest(preq) OK as.json4s.Json)
          ).either,
          kreq => http(createTopKRequest(kreq) OK as.json4s.Json).either
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

    val outstr = (s"duration: ${elapsedTime}\n" +
                  s"num_pred: ${numPreds}\n" +
                  s"num_top_k: ${numTopK}\n" +
                  s"num_obs: ${numObs}\n" +
                  s"pred_thru: ${pthruput}\n" +
                  s"obs_thru: ${othruput}\n" +
                  s"total_thru: ${totthruput}\n" +
                  s"successes: ${successes.size}\n" +
                  s"failures: ${failures.size}\n")
    Files.write(Paths.get("/home/ubuntu/velox-modelserver/client_output.txt"),
      outstr.getBytes(StandardCharsets.UTF_8))

    logWarning(outstr)
    println(outstr)

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


  // def parseWikiXML(xmlFile: String): mutable.HashMap[Int, String] = {
  //
  //   val corpus = new mutable.HashMap[Int, String]
  //   val xml = new XMLEventReader(Source.fromFile(xmlFile))
  //   var insidePage = false
  //   var buf = mutable.ArrayBuffer[String]()
  //   var index: Int = 0
  //   for (event <- xml) {
  //     event match {
  //       case EvElemStart(_, "page", _, _) => {
  //         insidePage = true
  //         val tag = "<page>"
  //         buf += tag
  //       }
  //       case EvElemEnd(_, "page") => {
  //         val tag = "</page>"
  //         buf += tag
  //         insidePage = false
  //         val s = buf.mkString
  //         corpus.put(index, s)
  //         index += 1
  //         buf.clear
  //       }
  //       case e @ EvElemStart(_, tag, _, _) => {
  //         if (insidePage) {
  //           buf += ("<" + tag + ">")
  //         }
  //       }
  //       case e @ EvElemEnd(_, tag) => {
  //         if (insidePage) {
  //           buf += ("</" + tag + ">")
  //         }
  //       }
  //       case EvText(t) => {
  //         if (insidePage) {
  //           buf += (t)
  //         }
  //       }
  //       case _ => // ignore
  //     }
  //   }
  // }

}

