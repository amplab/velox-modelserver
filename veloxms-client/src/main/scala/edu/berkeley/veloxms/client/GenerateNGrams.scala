
import java.nio.file.{Files, Paths}
import scala.collection.JavaConversions._

import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object GenerateNGrams {


  def main(args: Array[String]) {
    val dir = args(0)
    val files = Files.newDirectoryStream(Paths.get(dir)).filter(Files.isDirectory(_)).flatMap(Files.newDirectoryStream(_))
    val ngramSets = files.map(s => getNgrams(s.toAbsolutePath.toString))
    val allNgrams = ngramSets.reduce(_ ++ _)
    val result = allNgrams.mkString("\n")
    println(result)

  }

  def getNgrams(f: String): Set[String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val ns = Array(1,2,3)
    val text: String = Source.fromFile(f).mkString("")
    val unigrams = text.trim.toLowerCase.split("[\\p{Punct}\\s]+")
    ns.map(n => {
      unigrams.sliding(n).map(gram => gram.mkString(" "))
    }).flatMap(identity).toSet
  }

}
