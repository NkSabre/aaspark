package xyz.yjh.aaspark.ch04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD

object RDFPrediction extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AASpark ch04 Prediction with Decision Tree")
      .setExecutorEnv("--total-executor-cores", "32")
      .setExecutorEnv("--executor-memory", "32g")

    val sc = new SparkContext(conf)

    val rawData = sc.textFile("hdfs:///yjh/ch04/covtype.data", 32)

    firstTree(sc, rawData)
    secondTree(sc, rawData)
    randomForest(sc, rawData)
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }

  def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
    val countsByCategory = data.map(_.label).countByValue()
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }

  def firstTree(sc: SparkContext, rawData: RDD[String]): Unit = {
    val data = rawData.map{ line =>
      val values = line.split(',').map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    buildTree(data)( (trainData, cvData, testData) => {
      val model = DecisionTree.trainClassifier(
        trainData, 7, Map[Int, Int](), "gini", 4, 100)

      val metrics = getMetrics(model, cvData)

      // scalastyle:off
      println(metrics.confusionMatrix)
      println(metrics.accuracy)
      // println(metrics.recall)
      (0 until 7).map { i =>
        (metrics.precision(i), metrics.recall(i))
      }.foreach(println)
      // scalastyle:on

      val trainPriorProbabilities = classProbabilities(trainData)
      val cvPriorProbabilities = classProbabilities(cvData)
      val randomAccuracy = trainPriorProbabilities.zip(cvPriorProbabilities).map {
        case (trainProb, cvProb) => trainProb * cvProb
      }.sum
      // scalastyle:off
      print(randomAccuracy)
      // scalastyle:on

      val evaluations =
        for (impurity <- Array("gini", "entropy");
             depth <- Array(1, 20);
             bins <- Array(10, 300))
          yield {
            val model = DecisionTree.trainClassifier(
              trainData, 7, Map[Int, Int](), impurity, depth, bins)
            val accuracy = getMetrics(model, cvData).accuracy
            ((impurity, depth, bins), accuracy)
          }
      // scalastyle:off
      evaluations.sortBy(_._2).reverse.foreach(println)
      // scalastyle:on

      val bestModel = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), "entropy", 20, 300)
      // scalastyle:off
      println(getMetrics(bestModel, testData).accuracy)
      println(getMetrics(bestModel, trainData.union(cvData)).accuracy)
      // scalastyle:on
    })
  }

  private def buildCatalogFeature(rawData: RDD[String]) = {
    rawData.map{ line =>
      val values = line.split(',').map(_.toDouble)
      val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
      val soil = values.slice(14, 54).indexOf(1.0).toDouble
      val featureVector = Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }
  }

  def secondTree(sc: SparkContext, rawData: RDD[String]): Unit = {
    val data = buildCatalogFeature(rawData)

    buildTree(data)( (trainData, cvData, testData) => {
      val evaluations =
        for (impurity <- Array("gini", "entropy");
             depth <- Array(10, 20, 30);
             bins <- Array(40, 300))
          yield {
            val model = DecisionTree.trainClassifier(
              trainData, 7, Map(10 -> 4, 11 -> 40),
              impurity, depth, bins)
            val trainAccuracy = getMetrics(model, trainData).accuracy
            val cvAccuracy = getMetrics(model, cvData).accuracy
            ((impurity, depth, bins), (trainAccuracy, cvAccuracy))
          }

      // scalastyle:off
      evaluations.reverse.foreach(println)
      // scalastyle:on

      val bestModel = DecisionTree.trainClassifier(trainData.union(cvData), 7, Map(10 -> 4, 11 -> 40),
        "entropy", 30, 300)
      // scalastyle:off
      println(getMetrics(bestModel, testData).accuracy)
      // scalastyle:on
    })
  }

  def randomForest(sc: SparkContext, rawData: RDD[String]): Unit = {
    val data = buildCatalogFeature(rawData)

    buildTree(data)( (trainData, cvData, testData) => {
      val forest = RandomForest.trainClassifier(trainData, 7, Map(10 -> 4, 11 -> 40), 20,
        "auto", "entropy", 30, 300)

      val predictionsAndLabels = testData.map(example =>
        (forest.predict(example.features), example.label)
      )

      // scalastyle:off
      println(new MulticlassMetrics(predictionsAndLabels).accuracy)
      // scalastyle:on

      val input = "2709,125,28,67,23,3224,253,207,61,6094,0,29"
      val vector = Vectors.dense(input.split(',').map(_.toDouble))
      val label = forest.predict(vector)
      //scalastyle:off
      println(label)
      // scalastyle:on
    })
  }

  def buildTree(data: RDD[LabeledPoint])(f: (RDD[LabeledPoint], RDD[LabeledPoint], RDD[LabeledPoint]) => Unit): Unit = {
    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    f(trainData, cvData, testData)

    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()
  }

}
