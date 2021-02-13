import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SQLContext

import org.apache.spark.mllib.recommendation.ALS

object CF {

  //FUNCTION USED TO CREATE RATING OBJECT
  def ratingCreation(line: String): Rating = {
    val fields = line.split(',')
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

  def topRated() = {

  }


  def main(args: Array[String]): Unit = {

    //SET OF SPARK ENVIRONMENT
    val conf = new SparkConf().setAppName("CollFilt").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //DECLARATION OF DATASETS
    val data = sc.textFile("ratings.csv")
    data.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val movies = sc.textFile("movie.csv")

    //CREATION OF TUPLES
    val results = data.map(ratingCreation)
    val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()

    println("ArrivePoint1")

    //SPLIT OF RATINGS DATASET
    val Array(trainingset,dataset) = results.randomSplit(Array(0.9,0.1))
    println("ArrivePoint2")

    //SET OF ALS ALGORITHM
    val rank = 6; val iter = 5; val lambda = 0.01
    val algoALS = ALS.train(trainingset,rank,iter,lambda)


    val usersProducts = results.map { case Rating(user, product, rate) => (user, product)}
    //usersProducts.foreach(println)

    val predictions = algoALS.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)}
    //predictions.foreach(println)

    val ratesAndPreds = results.map { case Rating(user, product, rate) => ((user, product), rate)}.join(predictions)
    //ratesAndPreds.foreach(println)

    //MEAN SQUARED ERROR
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => val err = (r1 - r2)
      err * err
    }.mean()
    println(MSE)

    //EXAMPLE
    val predictRating = algoALS.predict(3,102)
    println(predictRating)

    val userId = 789
    val K = 10
    //val topKRecs = algoALS.recommendProducts(userId, K)
    //println(topKRecs.mkString("\n"))


  }
}

