import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

object CF {

  //SET OF SPARK ENVIRONMENT
  val conf = new SparkConf().setAppName("CollFilt").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  //DECLARATION OF DATASETS
  val data = sc.textFile("ratings.csv")
  val movies = sc.textFile("movie.csv")


  println("ArrivePoint1")

  println("ArrivePoint2")

  //FUNCTION USED TO CREATE RATING OBJECT
  def ratingCreation(line: String): Rating = {
    val fields = line.split(',')
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

  //ALGORITHM ALS
  def ALSAlgo(trainingset: RDD[Rating]): MatrixFactorizationModel = {
    val rank = 6; val iter = 5; val lambda = 0.01
    val algoALS = ALS.train(trainingset,rank,iter,lambda)
    algoALS
  }

  //
  def topRatedForKnownUser(user: Int, numberOfPrediction: Int,algoALS: MatrixFactorizationModel): Double = {
    val predictRating = algoALS.predict(user,numberOfPrediction)
    predictRating
  }

  def topRatedForUnknownUser(): Unit = {

  }

  //MEAN SQUARE ERROR OF THE MODEL
  def MSE(algoALS: MatrixFactorizationModel, usersProducts: RDD[(Int,Int)], results: RDD[Rating]): Double = {
    val predictions = algoALS.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)}
    predictions.foreach(println)
    val ratesAndPreds = results.map { case Rating(user, product, rate) => ((user, product), rate)}.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => val err = (r1 - r2)
      err * err
    }.mean()
    MSE
  }

  def makeAPrediction(arg1: Int, arg2: Int,algoALS: MatrixFactorizationModel): Long = {
    val predictRating = algoALS.predict(arg1,arg2)
    val prediction = predictRating.round
    prediction
  }

  def main(args: Array[String]): Unit = {
    //CREATION OF TUPLES
    val results = data.map(ratingCreation)
    val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
    //SPLIT OF RATINGS DATASET
    val Array(trainingset,dataset) = results.randomSplit(Array(0.9,0.1))
    trainingset

  }
}
