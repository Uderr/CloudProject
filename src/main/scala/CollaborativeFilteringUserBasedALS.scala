import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{desc, monotonically_increasing_id, rand, sum}
import scala.util.{Random, Try}


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

  //REMOVE LOGS FROM TERMINAL
  ss.sparkContext.setLogLevel("WARN")


  //DECLARATION OF DATASETS
  val data = sc.textFile("Dataset/rating.csv")
  val movies = sc.textFile("Dataset/movie.csv")

  val dataForRandom = ss.read.format("csv").option("header","true").load("DatasetWithID/rating.csv")
  val moviesForRandom = ss.read.format("csv").option("header","true").load("DatasetWithID/movie.csv")


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
    val movieName = moviesForRandom.where("movieId ==" + movie)
    val movieNameOnly = movieName.select("title")
    println(movieNameOnly)
    val test = movieNameOnly.toString()
    println("ciao " + test)

    println("The user: " + user + " will like the movie: " + movie + " with an aproximately prediction of: " + prediction)
    prediction
  }


  //----------------------------------------------------------------------------
  //MEAN SQUARE ERROR OF THE MODEL
  def MSE(algoALS: MatrixFactorizationModel,predictionWithMapping: RDD[((Int, Int), (Double, Double))]): Double = {

    val MSE = predictionWithMapping.map {
      case ((user, product), (r1, r2)) => val err = (r1 - r2)
      err * err}.mean()
    MSE
  }


  //----------------------------------------------------------------------------
  //COLD START - FUNCTIONS USED TO RESOLVE THE PROBLEM OF A NEW USER

  //FUNCTION USED FOR GENERATE 20 TOP RATED FILMS
  def topRated(): (Array[String], Array[Int]) = {
    import ss.implicits._

    val data = ss.read.format("csv").option("header", "true").load("DatasetWithID/rating.csv")
    val movies = ss.read.format("csv").option("header","true").load("DatasetWithID/movie.csv")
    val movie = movies.withColumn("movieId", $"movieId".cast("Integer"))

    println("Which of this top rated films do you like? ")

    val firstDataframe = data.groupBy("movieId").agg(sum("rating"))
      .withColumn("sum(rating)", $"sum(rating)" / 40144)
    val dataframeMovieRating = movie.join(firstDataframe,movie("movieId") === firstDataframe("movieId"),"inner").drop(firstDataframe("movieId"))
      .sort(desc("sum(rating)"))

    //dataframeMovieRating.show()

    val nameOfRecommendedFilm = dataframeMovieRating.select("title").collect.map(_.getString(0)).take(20)
    //nameOfRecommendedFilm.foreach(println)

    val idOfRecommendedFilm = dataframeMovieRating.select("movieId").collect.map(_.getInt(0)).take(20)
    //idOfRecommendedFilm.foreach(println)


    (nameOfRecommendedFilm,idOfRecommendedFilm)

  }

  //FUNCTION USED TO RECOMMEND 20 RANDOM MOVIES
  def randomRecommender(): (Array[String],Array[Int]) = {
    import ss.implicits._

    val data = ss.read.format("csv").option("header", "true").load("DatasetWithID/rating.csv")
    val movies = ss.read.format("csv").option("header","true").load("DatasetWithID/movie.csv")
    val movie = movies.withColumn("movieId", $"movieId".cast("Integer"))

    val onlySelectedUser = data.where("userId == 0")
    //onlySelectedUser.show()

    val dataframeWithoutUsers = data.except(onlySelectedUser)
    //dataframeWithoutUsers.show()


    val dataframeMovieRating = movie.join(dataframeWithoutUsers,movie("movieId") === dataframeWithoutUsers("movieId"),"inner").drop(dataframeWithoutUsers("movieId"))
    //dataframeMovieRating.show()

    val shuffledDF = dataframeMovieRating.orderBy(rand())
    //shuffledDF.show()

    val nameOfRandomFilm = shuffledDF.select("title").collect.map(_.getString(0)).take(20)
    //nameOfRandomFilm.foreach(println)

    val idOfRandomFilm = shuffledDF.select("movieId").collect.map(_.getInt(0)).take(20)
    //idOfRandomFilm.foreach(println)


    (nameOfRandomFilm,idOfRandomFilm)

  }


  //ASK A USER FEEDBACK FOR ALL TOP RATED AND RANDOM FILMS AND ADD THE NEW RATINGS TO THE DATASET
  //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  //CHECK SULL'INPUT = STRINGA
  //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  //ASK USER INPUT ABOUT RANDOM AND TOP RATED FILMS AND UPDATE THE MODEL
  def askUserInput(idAndName: (Array[String], Array[Int]),oldDataset: RDD[Rating]): RDD[Rating] = {
    import ss.implicits._

    val movieRecommended = idAndName._1
    val movieID = idAndName._2

    val ratingArr = new Array[Double](21)
    println("Insert rating (pay attention, rating must be between 0 and 5 in multiples of 0.5):")

    for(iteration<- 0 to 19) {
      print(movieRecommended(iteration))
      print(", how do you rate this film? ")
      var personalRate = scala.io.StdIn.readDouble()
      while((personalRate > 5 || personalRate < 1) || personalRate % 0.5 != 0) {
          println("Error: respect the parameters")
          print(movieRecommended(iteration))
          print(", how do you rate this film? ")
          personalRate = scala.io.StdIn.readDouble()
      }
    ratingArr(iteration) = personalRate
    }
    ratingArr.foreach((element:Double)=> print(element + " "))

    val userId = Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)


    val resultOfRating = sc.parallelize(ratingArr)
    val movieIdentification = sc.parallelize(movieID)
    val userID = sc.parallelize(userId)

    val dataframeResultOfRating = resultOfRating.map(rate => rate).toDF("rating")
    val dataframeMovieID = movieIdentification.map(a => a).toDF("movieId")
    val dataframeUserID = userID.map(a => a).toDF("userId")


    val datMovIDForJoin = dataframeMovieID.withColumn("id", monotonically_increasing_id)
    val dataUserIDForJoin = dataframeUserID.withColumn("id", monotonically_increasing_id)
    val dataRatForJoin = dataframeResultOfRating.withColumn("id",monotonically_increasing_id())


    //dataRatForJoin.show()
    val finalData1 = dataUserIDForJoin.join(datMovIDForJoin, Seq("id"))
    val finalData2 = finalData1.join((dataRatForJoin), Seq("id")).drop("id").orderBy("movieId")
    //finalData2.show()
    val finalDataRDDRating = finalData2.select("userId", "movieId", "rating").rdd.map(r => Rating(r.getInt(0), r.getInt(1),r.getDouble(2)))
    //finalDataRDDRating.foreach(println)

    val updatedDataset = oldDataset.union(finalDataRDDRating)

    updatedDataset.foreach(println)

    updatedDataset


  }

  //ADDING THE NEW RATINGS TO THE DATASET
  def updateModel(oldDataset: RDD[Rating], newDataset: RDD[Rating]): RDD[Rating] = {

    println("ArrivePoint")
    val updatedDataset = oldDataset.union(newDataset)
    updatedDataset

  }




  //----------------------------------------------------------------------------
  //MAIN FUNCTION

  def main(args: Array[String]): Unit = {

    //----------------------------------------------------------------------------
    //CREATION OF TUPLES
    val results = data.map(ratingCreation)
    val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()


    /*
    //----------------------------------------------------------------------------
    //SET OF THE ALS ALGORITHM
    val algo = ALSAlgo(results)

    //----------------------------------------------------------------------------
    //CALCULATION OF THE MEAN SQUARE ERROR
    val predictionsWithMapping = predictionWithMapping(results,algo) // PREDICTIONS OF ALL USERS COMPARED TO THEIR RATES
    //predictionsWithMapping.foreach(println)

    val mSError = MSE(algo,predictionsWithMapping)
    println("The mean square error of this model is: " + mSError)

    //----------------------------------------------------------------------------
    //PREDICTION FOR A KNOWN USER
    topRatedForKnownUser(algo, 1,titles) //PREDICTIONS FOR USER 1 (30 PREDICTION FROM BETTER TO WORST)

    val singlePrediction = makeAPredictionForAUserAndAFilm(1,31,algo) //SINGLE PREDICTION OF USER 1 FOR MOVIE 31
    println(singlePrediction)

     */
    //----------------------------------------------------------------------------
    //PREDICTION FOR NEW USER
    val topRatedFilms = topRated()
    val randomFilms = randomRecommender()

    //----------------------------------------------------------------------------
    //PREDICTION FOR NEW USER ACCORDING TO THE TOP RATED FILM'S RATINGS
    val newDatasetTopRated = askUserInput(topRatedFilms,results)
    val updatedDataset = updateModel(results,newDatasetTopRated)
    val newAlgoForTopRated = ALSAlgo(updatedDataset)


    println("First 30 recommended films based on your expressed preferences: ")
    topRatedForKnownUser(newAlgoForTopRated,0,titles)
    val singlePredictionForMe = makeAPredictionForAUserAndAFilm(0,329,newAlgoForTopRated)
    println(singlePredictionForMe)
    println("-------------------------------------------------")

    //----------------------------------------------------------------------------
    //PREDICTION FOR NEW USER ACCORDING TO TOP RATED AND RANDOM FILM'S RATINGS
    val newDatasetRandom = askUserInput(randomFilms,updatedDataset)
    val updatedAgain = updateModel(updatedDataset,newDatasetRandom)
    val newAlgoForRandom = ALSAlgo(updatedAgain)



  }
}
