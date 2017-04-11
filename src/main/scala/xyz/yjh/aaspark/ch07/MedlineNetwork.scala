package xyz.yjh.aaspark.ch07

import java.nio.charset.StandardCharsets

import scala.xml.{Elem, XML}

import com.cloudera.datascience.common.XmlInputFormat
import com.google.common.hash.Hashing
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object MedlineNetwork extends Serializable{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AASpark ch07 Analyzing Co-occurrence Networks with GraphX")
      .set("spark.cores.max", "16")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memory", "16g")

    val sc = new SparkContext(conf)
    
    val medline_raw = loadMedline(sc, "hdfs:///yjh/ch07")
    val mxml: RDD[Elem] = medline_raw.map(XML.loadString)
    val medline: RDD[Seq[String]] = mxml.map(majorTopics).cache()

    // scalastyle:off
    println(medline.take(1)(0))
    println(medline.count())

    val topics: RDD[String] = medline.flatMap(mesh => mesh).cache()
    val topicCounts = topics.countByValue()
    topicCounts.size
    val tcSeq = topicCounts.toSeq
    tcSeq.sortBy(_._2).reverse.take(10).foreach(println)
    val valueDist = topicCounts.groupBy(_._2).mapValues(_.size)
    valueDist.toSeq.sorted.take(10).foreach(println)
    // scalastyle:on

    val topicPairs = medline.flatMap(t => t.sorted.combinations(2))
    val cooccurs = topicPairs.map(p => (p, 1)).reduceByKey(_ + _)
    cooccurs.cache()
    cooccurs.count()

    // scalastyle:off
    cooccurs.top(10)(Ordering.by[(Seq[String], Int), Int](_._2)).foreach(println)

    val vertices = topics.map(topic => (hashId(topic), topic))
    val edges = cooccurs.map( p => {
      val (topics, cnt) = p
      val ids = topics.map(hashId).sorted
      Edge(ids(0), ids(1), cnt)
    })

    val topicGraph = Graph(vertices, edges)
    topicGraph.cache()

    println("TopicGraph vertices' count is " + topicGraph.vertices.count())
    println("TopicGraph edges' count is " + topicGraph.edges.count())

    val connectedComponentGraph: Graph[VertexId, Int] = topicGraph.connectedComponents()
    val componentCounts = sortedConnectedComponents(connectedComponentGraph)
    println("Connect ComponentGraph count: " + componentCounts.size)

    componentCounts.take(10).foreach(println)

    val nameCID = topicGraph.vertices.innerJoin(connectedComponentGraph.vertices) {
        (topicId, name, componentId) => (name, componentId)
      }
    val c1 = nameCID.filter(x => x._2._2 == componentCounts(1)._1)
    c1.collect().foreach(x => println(x._2._1))

    val coa = topics.filter(_.contains("CoA")).countByValue()
    coa.foreach(println)

    val degrees: VertexRDD[Int] = topicGraph.degrees.cache()
    val degreeStats = degrees.map(_._2).stats()
    println(degreeStats)
    topNamesAndDegrees(degrees, topicGraph).foreach(println)

    val T = medline.count()
    val topicCountRdd = topics.map(x => (hashId(x), 1)).reduceByKey( _ + _ )
    val topicCountGraph = Graph(topicCountRdd, topicGraph.edges)

    val chiSquaredGraph = topicCountGraph.mapTriplets(triplet => {
      chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
    })
    println(chiSquaredGraph.edges.map(x => x.attr).stats())

    val interesting = chiSquaredGraph.subgraph(triplet => triplet.attr > 19.5)
    println("Interesting Graph edges' count is " + interesting.edges.count())

    val interestingComponentCounts = sortedConnectedComponents(interesting.connectedComponents())
    println("Connect ComponentGraph of Interesting Graph count: " + interestingComponentCounts.size)
    interestingComponentCounts.take(10).foreach(println)

    val interestingDegrees: VertexRDD[Int] = interesting.degrees.cache()
    println(interestingDegrees.map(_._2).stats())
    topNamesAndDegrees(interestingDegrees, topicGraph).foreach(println)

    val avgCC = avgClusteringCoef(interesting)
    println("ClusteringCoef: " + avgCC)

    val paths = samplePathLengths(interesting)
    println(paths.map(_._3).filter(_ > 0).stats())

    val hist = paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)
    // scalastyle:on
  }

  def topNamesAndDegrees(degrees: VertexRDD[Int],
                         topicGraph: Graph[String, Int]): Array[(String, Int)] = {
    val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
      (topicId, degree, name) => (name, degree)
    }
    val ord = Ordering.by[(String, Int), Int](_._2)
    namesAndDegrees.map(_._2).top(10)(ord)
  }

  def avgClusteringCoef(graph: Graph[_, _]): Double = {
    val triCountGraph = graph.triangleCount()
    val maxTrisGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph) {
      (vertexId, triCount, maxTris) => if (maxTris == 0) 0 else triCount / maxTris
    }
    clusterCoefGraph.map(_._2).sum() / graph.vertices.count()
  }

  def samplePathLengths[V, E](graph: Graph[V, E], fraction: Double = 0.02): RDD[(VertexId, VertexId, Int)] = {
    val replacement = false
    val sample = graph.vertices.map(v => v._1).sample(
      replacement, fraction, 1729L)
    val ids = sample.collect().toSet

    val mapGraph = graph.mapVertices((id, v) => {
      if (ids.contains(id)) {
        Map(id -> 0)
      } else {
        Map[VertexId, Int]()
      }
    })

    val start = Map[VertexId, Int]()
    val res = mapGraph.ops.pregel(start)(update, iterate, mergeMaps)
    res.vertices.flatMap { case (id, m) =>
      m.map { case (k, v) =>
        if (id < k) {
          (id, k, v)
        } else {
          (k, id, v)
        }
      }
    }.distinct().cache()
  }

  def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
    def minThatExists(k: VertexId): Int = {
      math.min(
        m1.getOrElse(k, Int.MaxValue),
        m2.getOrElse(k, Int.MaxValue))
    }

    (m1.keySet ++ m2.keySet).map {
      k => (k, minThatExists(k))
    }.toMap
  }

  def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int])
      : Map[VertexId, Int] = {
    mergeMaps(state, msg)
  }

  def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId)
      : Iterator[(VertexId, Map[VertexId, Int])] = {
    val aplus = a.map { case (v, d) => v -> (d + 1) }
    if (b != mergeMaps(aplus, b)) {
      Iterator((bid, aplus))
    } else {
      Iterator.empty
    }
  }

  def iterate(e: EdgeTriplet[Map[VertexId, Int], _]): Iterator[(VertexId, Map[VertexId, Int])] = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
      checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
  }


  def loadMedline(sc: SparkContext, path: String): RDD[String] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XmlInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
    in.map(line => line._2.toString)
  }

  def majorTopics(elem: Elem): Seq[String] = {
    val dn = elem \\ "DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    mt.map(n => n.text)
  }

  def sortedConnectedComponents(connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
    val componentCounts = connectedComponents.vertices.map(_._2).countByValue
    componentCounts.toSeq.sortBy(_._2).reverse
  }

  def hashId(str: String): Long = {
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).asLong()
  }

  def chiSq(YY: Int, YB: Int, YA: Int, T: Long): Double = {
    val NB = T - YB
    val NA = T - YA
    val YN = YA - YY
    val NY = YB - YY
    val NN = T - NY - YN - YY
    val inner = math.abs(YY * NN - YN * NY) - T / 2.0
    T * math.pow(inner, 2) / (YA * NA * YB * NB)
  }
}
