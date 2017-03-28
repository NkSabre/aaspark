package xyz.yjh.aaspark.ch03

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


object Recommender {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AASpark ch03 Recommender")
      .setExecutorEnv("--total-executor-cores", "16")
      .setExecutorEnv("--executor-memory", "16g")
    
    val sc = new SparkContext(conf)

    val rawUserArtistData = sc.textFile("hdfs:///yjh/ch03/user_artist_data.txt", 16)
    val userIDStats = rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    val artistIDStats = rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
    // scalastyle:off
    println(userIDStats)
    println(artistIDStats)
    // scalastyle:on
    val rawArtistData = sc.textFile("hdfs:///yjh/ch03/artist_data.txt", 16)
    val rawArtistAlias = sc.textFile("hdfs:///yjh/ch03/artist_alias.txt", 16)

    rough_model(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
    evaluate(sc, rawUserArtistData, rawArtistAlias)
    recommend(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
  }

  def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int, Int] = {
    rawArtistAlias.flatMap{ line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()
  }

  def buildArtistByID(rawArtistData: RDD[String]): RDD[(Int, String)] = {
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }
  }

  def buildRatings(
                    rawUserArtistData: RDD[String],
                    bArtistAlias: Broadcast[Map[Int, Int]]): RDD[Rating] = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def rough_model(sc: SparkContext, rawUserArtistData: RDD[String],
                  rawArtistData: RDD[String], rawArtistAlias: RDD[String]): Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildRatings(rawUserArtistData, bArtistAlias).cache()

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    trainData.unpersist()

    val userId = 2093760
    val rawArtistForUser = rawUserArtistData.map(_.split(' ')).filter {
      case Array(user, _, _) => user.toInt == userId
    }

    val existingProducts = rawArtistForUser.map { case Array(_, artist, _) => artist.toInt }
      .collect().toSet

    val recommendations = model.recommendProducts(userId, 5)
    // scalastyle:off
    recommendations.foreach(println)
    // scalastyle:on

    val recommededProcuctIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)
    // scalastyle:off
    artistByID.filter{ case (id, name) =>
      existingProducts.contains(id)
    }.values.collect().foreach(println)
    artistByID.filter { case (id, name) =>
      recommededProcuctIDs.contains(id)
    }.values.collect().foreach(println)
    // scalastyle:on

    unpersist(model)
  }

  def areaUnderCurve(positiveData: RDD[Rating],
                     bAllItemIDs: Broadcast[Array[Int]],
                     predictFunction: (RDD[(Int, Int)] => RDD[Rating])): Double = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.length && negative.length < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.length))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def predictMostListened(sc: SparkContext,
                          train: RDD[Rating])(allData: RDD[(Int, Int)]): RDD[Rating] = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def evaluate( sc: SparkContext,
                rawUserArtistData: RDD[String],
                rawArtistAlias: RDD[String]): Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val allData = buildRatings(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    // scalastyle:off
    println(mostListenedAUC)
    // scalastyle:on

    val evaluations =
      for (rank <- Array(10, 50);
           lambda <- Array(1.0, 0.0001);
           alpha <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
          val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
          unpersist(model)
          ((rank, lambda, alpha), auc)
        }
    // scalastyle:off
    evaluations.sortBy(_._2).reverse.foreach(println)
    // scalastyle:on
    
    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(
                 sc: SparkContext,
                 rawUserArtistData: RDD[String],
                 rawArtistData: RDD[String],
                 rawArtistAlias: RDD[String]): Unit = {

    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    allData.unpersist()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    // scalastyle:off
    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      values.collect().foreach(println)
    // scalastyle:off

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5))
    // scalastyle:off
    someRecommendations.map(
      recs => recs.head.user + " -> " + recs.map(_.product).mkString(", ")
    ).foreach(println)
    // scalastyle:on

    unpersist(model)
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }
}
