package xyz.yjh.aaspark.ch06

import java.io.{FileOutputStream, PrintStream}
import java.util.Properties

import scala.collection.{mutable => m}
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

import com.cloudera.datascience.common.XmlInputFormat
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

object WikiLSA extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AASpark ch06 Understanding Wikipedia with Latent Semantic Analysis")
      .set("spark.executor.cores", "8")
      .set("spark.executor.memory", "32g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.network.timeout", "360s")

    val sc = new SparkContext(conf)

    val sampleSize = 0.1
    val numTerms = 50000
    val k = 100

    val (termDocMatrix, idTerms, docIds, idfs) = preprocessing(sampleSize, numTerms, sc)

    /*
    val path = "hdfs:///yjh/ch06/wikidump.xml"

    val rawXmls = readAndSplitHugeXml(path, sc)
    val plainText = rawXmls.flatMap(wikiXmlToPlanText)

    val stopWords = sc.broadcast(scala.io.Source.fromFile("stopwords.txt").getLines().toSet).value
    val lemmatized: RDD[Seq[String]] = plainText.mapPartitions( it => {
      val pipeline = createNLPPipeline()
      it.map{ case (title, contents) =>
        plainTextToLemmas(contents, stopWords, pipeline)
      }
    })

    val docTermFreqs = lemmatized.map( terms => {
      val termFreqs = terms.foldLeft(new m.HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      termFreqs
    })

    docTermFreqs.cache()

    // documentFrequencies(docTermFreqs)

    val idfs = inverseDocumentFrequencies(topDocFreqs(docTermFreqs, 50000), docTermFreqs.count())
    val termIds = idfs.keys.zipWithIndex.toMap

    val idTerms = termIds.map(_.swap)

    val bTermIds = sc.broadcast(termIds).value
    val bIdfs = sc.broadcast(idfs).value

    val termDocMatrix = documentTermMatrix(docTermFreqs, bTermIds, bIdfs).cache()     */

    val mat = new RowMatrix(termDocMatrix)
    val svd = mat.computeSVD(k, computeU = true)

    val topConceptTerms = topTermsInTopConcepts(svd, 10, 10, idTerms)
    val topConceptDocs = topDocsInTopConcepts(svd, 10, 10, docIds)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      // scalastyle:off
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
      // scalastyle:on
    }
  }

  def preprocessing(sampleSize: Double, numTerms: Int, sc: SparkContext)
          : (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {

    val path = "hdfs:///yjh/ch06/wikidump.xml"

    val pages = readAndSplitHugeXml(path, sc).sample(withReplacement = false, sampleSize, 11L)

    val plainText = pages.filter(_ != null).flatMap(wikiXmlToPlainText)

    val stopWords = sc.broadcast(loadStopWords("stopwords.txt")).value

    val lemmatized = plainText.mapPartitions(iter => {
      val pipeline = createNLPPipeline()
      iter.map{ case(title, contents) => (title, plainTextToLemmas(contents, stopWords, pipeline))}
    })

    val filtered = lemmatized.filter(_._2.size > 1)

    documentTermMatrix(filtered, stopWords, numTerms, sc)
  }

  def loadStopWords(path: String): Set[String] = {
    scala.io.Source.fromFile(path).getLines().toSet
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                            numTerms: Int, idTerms: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(_._1)
      topTerms += sorted.take(numTerms).map{
        case (score, id) => (idTerms(id), score)
      }
    }

    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                           numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topDocs += docWeights.take(numDocs).map{
        case (score, id) => (docIds(id), score)
      }
    }

    topDocs
  }

  def documentTermMatrix(docs: RDD[(String, Seq[String])], stopWords: Set[String], numTerms: Int,
                         sc: SparkContext): (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {

    val docTermFreqs = docs.mapValues( terms => {
      val termFreqs = terms.foldLeft(new m.HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      termFreqs
    })

    docTermFreqs.cache()

    // documentFrequencies(docTermFreqs)
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    val docFreqs = topDocFreqs(docTermFreqs.map(_._2), numTerms)
    // scalastyle:off
    println("Number of terms: " + docFreqs.length)
    // scalastyle:on
    saveDocFreqs("docfreqs.tsv", docFreqs)

    val idfs = inverseDocumentFrequencies(topDocFreqs(docTermFreqs.map(_._2), numTerms), docTermFreqs.count())
    val termIds = idfs.keys.zipWithIndex.toMap

    val idTerms = termIds.map(_.swap)

    val bTermIds = sc.broadcast(termIds).value
    val bIdfs = sc.broadcast(idfs).value

    val matrix = docTermFreqs.map(_._2).map( termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, freq) => bTermIds.contains(term)
      }.map{
        case (term, freq) => (bTermIds(term), bIdfs (term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bTermIds.size, termScores)
    })

    (matrix, idTerms, docIds, idfs)
  }

  def documentFrequencies(docTermFreqs: RDD[m.HashMap[String, Int]]): m.HashMap[String, Int] = {
    val zero = new m.HashMap[String, Int]()
    def merge(dfs: m.HashMap[String, Int], tfs: m.HashMap[String, Int]): m.HashMap[String, Int] = {
      tfs.keySet.foreach { term =>
        dfs += term -> (dfs.getOrElse(term, 0) + 1)
      }
      dfs
    }
    def comb(dfs1: m.HashMap[String, Int], dfs2: m.HashMap[String, Int]): m.HashMap[String, Int] = {
      for ((term, count) <- dfs2) {
        dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
      }
      dfs1
    }
    docTermFreqs.aggregate(zero)(merge, comb)
  }

  def topDocFreqs(docTermFreqs: RDD[m.HashMap[String, Int]], numTerms: Int): Array[(String, Int)] = {
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)
    val ordering = Ordering.by[(String, Int), Int](_._2)
    docFreqs.top(numTerms)(ordering)
  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Long): Map[String, Double] = {
    docFreqs.map{
      case (term, count) => (term, math.log(numDocs.toDouble / count))
    }.toMap
  }

  def readAndSplitHugeXml(path: String, sc: SparkContext): RDD[String] = {
    @transient  val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")

    val kvs = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)

    kvs.map(p => p._2.toString)
  }

  def wikiXmlToPlainText(xml: String): Option[(String, String)] = {
    val page = new EnglishWikipediaPage()
    WikipediaPage.readPage(page, xml)

    if(page.isEmpty || !page.isArticle || page.isRedirect || page.getTitle.contains("(disambiguation)")) {
      None
    } else {
      Some((page.getTitle, page.getContent))
    }
  }

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => c.isLetter)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)

    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences.asScala;
         token <- sentence.get(classOf[TokensAnnotation]).asScala) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  def saveDocFreqs(path: String, docFreqs: Array[(String, Int)]) {
    val ps = new PrintStream(new FileOutputStream(path))
    for ((doc, freq) <- docFreqs) {
      // scalastyle:off
      ps.println(s"$doc\t$freq")
      // scalastyle:on
    }
    ps.close()
  }
}
