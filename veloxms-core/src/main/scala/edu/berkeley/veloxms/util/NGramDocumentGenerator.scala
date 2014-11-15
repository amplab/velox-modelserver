package edu.berkeley.veloxms.util

import scala.util.Random
import scala.collection.mutable
import scala.io.Source

object NGramDocumentGenerator {

  /**
   * @param length the number of ngrams to use
   */
  def generateDoc(length: Int, ngrams: Array[String]): String = {
    val rand = new Random(System.currentTimeMillis)
    val docAsList = new Array[String](length)
    var i = 0
    while (i < length) {
      docAsList(i) = ngrams(rand.nextInt(ngrams.size))
      i += 1
    }
    val doc = docAsList.mkString(" ")
    doc
  }

  def readNgramFile(file: String): Array[String] = {
    val ngrams = Source.fromFile(file)
      .getLines
      .toArray
    ngrams
  }

  def createCorpus(numDocs: Int, docLength: Int, ngramsFile: String): Array[String] = {
    val ngrams = readNgramFile(ngramsFile)

    val corpus = new Array[String](numDocs)
    var i = 0
    while (i < numDocs) {
      corpus(i) = generateDoc(docLength, ngrams)
      i += 1
    }
    corpus
  }

}
