import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{desc, sum}


object CollaborativeFilteringUserBasedALS {

  //SET OF SPARK ENVIRONMENT
  val conf = new SparkConf().setAppName("CollFilt").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val ss = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  //DECLARATION OF DATASETS
  val data = sc.textFile("rating.csv")
  val movies = sc.textFile("movie.csv")


  //----------------------------------------------------------------------------
  //SETTING OF THE ALGORITHM ALS
  def ALSAlgo(trainingset: RDD[Rating]): MatrixFactorizationModel = {
    val rank = 6; val iter = 5; val lambda = 0.01
    val algoALS = ALS.train(trainingset,rank,iter,lambda)
    algoALS
  }


  //----------------------------------------------------------------------------
  //SET OF FUNCTIONS USED TO CREATE MAPPED VALUES


  //DEFINITION OF A FUNCTION THAT, RECEIVING AN RDD[String] RETURN A Rating
  def ratingCreation(line: String): Rating = {
    val fields = line.split(',')
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

  //DEFINITION OF A FUNCTION THAT, RECEIVING AN RDD AND A MODEL RETURN THE RDD CONTAINING THE PREDICTIONS AND THE RATE FOR A USER,ITEM KEY-VALUE
  def predictionWithMapping(ratings: RDD[Rating], algoALS: MatrixFactorizationModel): RDD[((Int, Int), (Double, Double))] = {
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product)
    }
    val predictions = algoALS.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)
    }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)
    ratesAndPreds
  }

  //DEFINITION OF A FUNCTION THAT, RECEIVING IN INPUT AND RDD AND A MODEL RETURN THE PREDICTIONS FOR A USER,ITEM KEY-VALUE
  def predictionWithoutMapping(ratings: RDD[Rating], algoALS: MatrixFactorizationModel): RDD[((Int,Int),Double)]= {
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product)
    }
    val predictions = algoALS.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)
    }
    predictions
  }


  //----------------------------------------------------------------------------
  //SET OF FUNCTIONS USED FOR THE RECOMMENDATIONS


  //FUNCTION USED FOR RETURN THE n TOP RATED ITEM FOR THE USER u
  def topRatedForKnownUser(algoALS: MatrixFactorizationModel, user: Int, titles: scala.collection.Map[Int,String]): Array[Rating] = {
    val x = 30
    val recommendProducts = algoALS.recommendProducts(user,x)
    println("Top " + x + " film suggested for user " + user + ":")
    recommendProducts.map(rating => (titles(rating.product))).foreach(println)
    recommendProducts

  }

  //FUNCTION THAT, TAKING IN INPUT AN OBJECT ID AND A USER ID, RETURN THE PREDICTION OF RATING FOR THAT OBJECT
  def makeAPredictionForAUserAndAFilm(user: Int, movie: Int,algoALS: MatrixFactorizationModel): Long = {
    val predictRating = algoALS.predict(user,movie)
    val prediction = predictRating.round
    //val recommendWIthNames =
    println("The user: " + user + " will like the movie: " + movie + " with an aproximately prediction of: " + prediction)
    prediction
  }


  //----------------------------------------------------------------------------
  //MEAN SQUARE ERROR OF THE MODEL
  def MSE(algoALS: MatrixFactorizationModel,predictionWithMapping: RDD[((Int, Int), (Double, Double))]): Double = {

    val MSE = predictionWithMapping.map { case ((user, product), (r1, r2)) => val err = (r1 - r2)
      err * err}.mean()
    MSE
  }


  //----------------------------------------------------------------------------
  //COLD START - FUNCTIONS USED TO RESOLVE THE PROBLEM OF A NEW USER

  //FUNCTION USED FOR GENERATE 20 TOP RATED FILMS
  def topRated(): Array[String] = {
    import ss.implicits._

    val data = ss.read.format("csv").option("header", "true").load("DatasetWithID/rating.csv")
    val movies = ss.read.format("csv").option("header","true").load("DatasetWithID/movie.csv")
    val movie = movies.withColumn("movieId", $"movieId".cast("Integer"))


    val firstDataframe = data.groupBy("movieId").agg(sum("rating"))
      .withColumn("sum(rating)", $"sum(rating)" / 40144)
    val dataframeMovieRating = movie.join(firstDataframe,movie("movieId") === firstDataframe("movieId"),"inner")
      .sort(desc("sum(rating)"))


    val nameWithRating = dataframeMovieRating.select("title","sum(rating)").take(20)

    val nameOfRecommendedFilm = dataframeMovieRating.select("title").collect.map(_.getString(0)).take(20)

    nameOfRecommendedFilm

  }

  //ASK A USER FEEDBACK FOR ALL TOP RATED FILMS AND ADD THE NEW RATINGS TO THE DATASET
  def askUserInput(topRated: Array[String]): Array[Int] = {
    val feedback = Array[Int](30)
    for(i <- 5 to 10) {

      feedback(i) = i
    }
    println(feedback)
    feedback
  }

  //ASK A USER FEEDBACK FOR 10 RANDOM MOVIES AND ADD THE NEW RATINGS TO THE DATASET
  def addFilmForNewUser(): Unit = {

  }

  //MAKE A PREDICTION WITH THE NEW RATINGS THAT THE NEW USER ADD
  def predictionForNewUser(): Unit = {

  }

  
  //----------------------------------------------------------------------------
  //MAIN FUNCTION

  def main(args: Array[String]): Unit = {

    //CREATION OF TUPLES
    val results = data.map(ratingCreation)
    val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()


    /*
    val algo = ALSAlgo(results)

    val predictionsWithoutMapping = predictionWithoutMapping(results,algo) // PREDICTIONS OF ALL USERS
    //predictionsWithoutMapping.foreach(println)

    val predictionsWithMapping = predictionWithMapping(results,algo) // PREDICTIONS OF ALL USERS COMPARED TO THEIR RATES
    //predictionsWithMapping.foreach(println)

    topRatedForKnownUser(algo, 1,titles) //PREDICTIONS FOR USER 1 (30 PREDICTION FROM BETTER TO WORST)


    val singlePrediction = makeAPredictionForAUserAndAFilm(1,31,algo) //SINGLE PREDICTION OF USER 1 FOR MOVIE 31


    val mSError = MSE(algo,predictionsWithMapping)
    println("The mean square error of this model is: " + mSError)


     */

    topRated()

  }
}
