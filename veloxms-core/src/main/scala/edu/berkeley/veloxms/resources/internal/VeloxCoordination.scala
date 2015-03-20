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

case class HDFSLocation(loc: String)

class WriteToHDFSServlet(model: Model[_, _], timer: Timer, sparkMaster: String, partition: Int) extends HttpServlet
  with Logging {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {
      val obsLocation = jsonMapper.readValue(req.getInputStream, classOf[HDFSLocation])
      // TODO make sure that hadoop-site.xml, etc. are on classpath
      System.setProperty("HADOOP_USER_NAME", "root")
      val conf = new Configuration()
      conf.addResource(new Path("/home/ubuntu/velox-modelserver/conf/core-site.xml"))
      logInfo(conf.toString())
      val uri = s"hdfs://$sparkMaster:9000/${obsLocation.loc}/part_$partition"

      // conf.set("fs.defaultFS", uri)
      val path = new Path(uri)
      val fs = FileSystem.get(conf)
      val overwrite = true
      val outFile = fs.create(path, overwrite) // overwrite existing file
      val observations = IOUtils.toInputStream(model.getObservationsAsCSV.mkString("\n"))
      IOUtils.copy(observations, outFile)
      fs.close()
      resp.setContentType("application/json");
      jsonMapper.writeValue(resp.getOutputStream, "success")
    } finally {
      timeContext.stop()
    }
  }

}

// object Hdfs {
//   def write(uri: String, filePath: String, data: Array[Byte]): Unit = {
//     System.setProperty("HADOOP_USER_NAME", "root")
//     val path = new Path(filePath)
//     val conf = new Configuration()
//     conf.set("fs.defaultFS", uri)
//     val fs = FileSystem.get(conf)
//     val os = fs.create(path)
//     os.write(data)
//     fs.close()
//   }

class LoadNewModelServlet(model: Model[_, _], timer: Timer, sparkMaster: String)
    extends HttpServlet with Logging {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    val modelLocation = jsonMapper.readValue(req.getInputStream, classOf[HDFSLocation])
    try {
      System.setProperty("HADOOP_USER_NAME", "root")
      val conf = new Configuration()
      conf.addResource(new Path("/home/ubuntu/velox-modelserver/conf/core-site.xml"))
      logInfo(conf.toString())
      val uri = s"hdfs://$sparkMaster:9000/${modelLocation.loc}/"
      val fs = FileSystem.get(conf)
      val itemModelsPath = new Path(s"$uri/items")
      val itemModelsInputStream = fs.open(itemModelsPath)
      val itemModelStrings = new StringWriter()
      IOUtils.copy(itemModelsInputStream, itemModelStrings)
      val items = itemModelStrings.toString().split("\n").map(line => {
        val itemSplits = line.split(", ")
        val itemId = itemSplits(0).toLong
        val itemFeatures: Array[Double] = itemSplits.drop(1).map(_.toDouble).toArray
        // TODO this should be inserted into storage backend
        (itemId, itemFeatures)
      }).toMap
      val firstItem = items.head
      logInfo(s"Loaded new models for ${items.size} items. " +
        s"First one is:\n${firstItem._1}, ${firstItem._2.mkString(", ")}")

      // TODO only add users in this partition: if (userId % partNum == 0)
      // val newUserMap = new ConcurrentHashMap[Long, WeightVector]
      //
      val userModelsPath = new Path(s"$uri/users")
      val userModelsInputStream = fs.open(userModelsPath)
      val userModelStrings = new StringWriter()
      IOUtils.copy(userModelsInputStream, userModelStrings)
      val users = userModelStrings.toString().split("\n").map(line => {
        val userSplits = line.split(", ")
        val userId = userSplits(0).toLong
        val userFeatures: Array[Double] = userSplits.drop(1).map(_.toDouble).toArray
        // TODO this should be inserted into storage backend
        (userId, userFeatures)
      }).toMap

      val firstUser = users.head
      logInfo(s"Loaded new models for ${users.size} users. " +
        s"First one is:\n${firstUser._1}, ${firstUser._2.mkString(", ")}")


      // TODO figure out best way to switch to new user and item maps
      // should probably be atomic

      fs.close()
      resp.setContentType("application/json");
      jsonMapper.writeValue(resp.getOutputStream, "success")

    } finally {

      timeContext.stop()
    }
  }

}












