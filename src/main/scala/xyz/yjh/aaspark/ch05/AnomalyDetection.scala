package xyz.yjh.aaspark.ch05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object AnomalyDetection extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AASpark ch05 Anomaly Detection in Network Traffic with K-means Clustering")
      .set("spark.executor.cores", "8")
      .set("spark.executor.memory", "32g")

    val sc = new SparkContext(conf)

    val rawData = sc.textFile("hdfs:///yjh/ch05/kddcup.data", 32)

    // scalastyle:off
    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
    // scalastyle:on

    firstRun(rawData)
    secondRun(rawData)
    thirdRun(rawData)
    fourthRun(rawData)
    fifthRun(rawData)
    clusterInAction(rawData)
  }

  def firstRun(rawData: RDD[String]): Unit = {
     val labelsAndData = rawData.map { line =>
       val buffer = line.split(',').toBuffer
       buffer.remove(1, 3)
       val label = buffer.remove(buffer.length - 1)
       val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
       (label, vector)
     }

    val data = labelsAndData.values.cache()

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    // scalastyle:off
    model.clusterCenters.foreach(println)
    // scalastyle:on

    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue

    clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster, label), count) =>
        // scalastyle:off
        println(f"$cluster%1s$label%18s$count%8s")
      // scalastyle:on
    }

    data.unpersist()
  }

  def secondRun(rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    // scalastyle:off
    (5 to 40 by 5).map(k => clusteringScore(data, k)).foreach(println)

    (30 to 100 by 10).par.map(k => (k, clusteringScoreWithSmallEpsilon(data, k))).toList.foreach(println)
    // scalastyle:on

    data.unpersist()
  }

  def distance(a: Vector, b: Vector) : Double =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel): Double = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(datum, centroid)
  }

  def clusteringScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringScoreWithSmallEpsilon(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def visualizatedR(rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    val kmeans = new KMeans()
    kmeans.setK(100)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)

    val sample = data.map(datum =>
      model.predict(datum) + "," + datum.toArray.mkString(",")
    ).sample(withReplacement = false, 0.05)

    sample.saveAsTextFile("hdfs:///yjh/ch05/sample")
  }

  private def normalizeFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce((a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.fold(
      new Array[Double](numCols)
    )(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2)
    )

    val stdevs = sumSquares.zip(sums).map {
      case(sumSq, sum) => math.sqrt(n*sumSq - sum*sum)/n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0) value - mean else (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }

  def thirdRun(rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    val normalizedData = data.map(normalizeFunction(data)).cache()
    // scalastyle:off
    (60 to 120 by 10).par.map(k =>
      (k, clusteringScore(normalizedData, k))).toList.foreach(println)
    // scalastyle:on

    data.unpersist()
  }

  private def categoricalAndLabelFunction(rawData: RDD[String]): (String => (String, Vector)) = {
    val splitData = rawData.map(_.split(','))
    val protocolTypes = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap

    (line: String) => {
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)

      val vector = buffer.map(_.toDouble)
      val protocalArray = new Array[Double](protocolTypes.size)
      protocalArray(protocolTypes(protocol)) = 1.0
      val serviceArray = new Array[Double](services.size)
      serviceArray(services(service)) = 1.0
      val tcpStateArray = new Array[Double](tcpStates.size)
      tcpStateArray(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, tcpStateArray)
      vector.insertAll(1, serviceArray)
      vector.insertAll(1, protocalArray)

      (label, Vectors.dense(vector.toArray))
    }
  }

  def fourthRun(rawData: RDD[String]): Unit = {
    val labelAndData = rawData.map(categoricalAndLabelFunction(rawData))
    val data = labelAndData.values

    val normalizedData = data.map(normalizeFunction(data)).cache()
    // scalastyle:off
    (80 to 160 by 10).map(k =>
      (k, clusteringScoreWithSmallEpsilon(normalizedData, k))).toList.foreach(println)
    // scalastyle:on

    normalizedData.unpersist()
  }

  private def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n: Double = values.sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }

  private def clusteringScoreWithEntropy(normalizedLabelsAndData: RDD[(String, Vector)], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setEpsilon(1.0e-6)

    val model = kmeans.run(normalizedLabelsAndData.values)
    val labelAndClusters = normalizedLabelsAndData.mapValues(model.predict)
    val clusterAndLabels = labelAndClusters.map(_.swap)

    val labelsInClusters = clusterAndLabels.groupByKey().values
    val labelCounts = labelsInClusters.map(_.groupBy(l => l).map(_._2.size))

    val n = normalizedLabelsAndData.count()

    labelCounts.map(m => m.sum * entropy(m)).sum() / n
  }

  def fifthRun(rawData: RDD[String]): Unit = {
    val labelAndData = rawData.map(categoricalAndLabelFunction(rawData))

    val normalizedLabelsAndData = labelAndData.mapValues(normalizeFunction(labelAndData.values)).cache()
    // scalastyle:off
    (80 to 160 by 10).map(k =>
      (k, clusteringScoreWithEntropy(normalizedLabelsAndData, k))).toList.foreach(println)
    // scalastyle:on

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedLabelsAndData.values)

    val clusterLabelCount = normalizedLabelsAndData.map { case (label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue

    clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster, label), count) =>
        // scalastyle:off
        println(f"$cluster%1s$label%18s$count%8s")
      // scalastyle:on
    }

    normalizedLabelsAndData.unpersist()
  }

  private def buildDector(data: RDD[Vector], normalizedFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizedFunction)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()
            
    val distances = normalizedData.map(datum => distToCentroid(datum, model))
    val threshold = distances.top(100).last

    (datum: Vector) => {
      distToCentroid(normalizedFunction(datum), model) > threshold
    }
  }

  def clusterInAction(rawData: RDD[String]): Unit = {
    val lineProcesser = categoricalAndLabelFunction(rawData)

    val originalAndData = rawData.map(line => (line, lineProcesser(line)._2))
    val data = originalAndData.values

    val normalizer = normalizeFunction(data)

    val dector = buildDector(data, normalizer)

    val anomalies = originalAndData.filter{
      case (original, datum) => dector(datum)
    }.keys
  }
}
