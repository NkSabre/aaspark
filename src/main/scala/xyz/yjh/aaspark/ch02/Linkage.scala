package xyz.yjh.aaspark.ch02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class MatchData(id1: Int, id2: Int,
                     scores: Array[Double], matched: Boolean)
case class Scored(md: MatchData, score: Double)

object Linkage extends Serializable  {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("AASpark ch02 Intro"))

    val rawblocks = sc.textFile("hdfs:///yjh/linkage")

    def isHeader(line: String) = line.contains("id_1")

    val noheader = rawblocks.filter(!isHeader(_))

    def toDouble(s: String) = {
      if ("?".equals(s)) Double.NaN else s.toDouble
    }

    def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed = noheader.map(parse(_))

    parsed.cache()

    val matchCounts = parsed.map(_.matched).countByValue()
    val matchCountsSeq = matchCounts.toSeq
    // scalastyle:off
    matchCountsSeq.sortBy(_._1).foreach(println)
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)
    // scalastyle:on

    import java.lang.Double.isNaN
    val stats = (0 until 9).map(i => {
      parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
    })
    // scalastyle:off
    stats.foreach(println)
    // scalastyle:on

    val nasRDD = parsed.map(md => {
      md.scores.map(d => NAStatCounter(d))
    })

    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map{ case (a, b) => a.merge(b)}
    })
    // scalastyle:off
    reduced.foreach(println)
    // scalastyle:on

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

    // scalastyle:off
    statsm.zip(statsn).map{ case (m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)
    // scalastyle:on

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    // scalastyle:off
    ct.filter(_.score >= 4.0).map(_.md.matched).countByValue().foreach(println)
    ct.filter(_.score >= 2.0).map(_.md.matched).countByValue().foreach(println)
    // scalastyle:on
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}
