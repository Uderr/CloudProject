import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{desc, lit, monotonically_increasing_id, rand, sum}

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
    val x = 20
    val recommendProducts = algoALS.recommendProducts(user,x)
    println("Top " + x + " film suggested for user " + user + ":")
    recommendProducts.map(rating => (titles(rating.product))).foreach(println)
    recommendProducts

  }

  //FUNCTION THAT, TAKING IN INPUT AN OBJECT ID AND A USER ID, RETURN THE PREDICTION OF RATING FOR THAT OBJECT
  def makeAPredictionForAUserAndAFilm(user: Int, movie: Int,algoALS: MatrixFactorizationModel): Unit = {
    val predictRating = algoALS.predict(user,movie)
    val prediction = predictRating.round
    val movieNameRow = moviesForRandom.where("movieId ==" + movie)
    val movieNameOnly = movieNameRow.select("title").collect.map(_.getString(0))

    println("The user: " + user + " will like the movie: " + movieNameOnly(0) + " with an aproximately prediction of: " + prediction)
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

    println("------------------------ TOP RATED PREFERENCES ------------------------ ")

    val firstDataframe = data.groupBy("movieId").agg(sum("rating"))
      .withColumn("sum(rating)", $"sum(rating)" / 40144)
    val dataframeMovieRating = movie.join(firstDataframe,movie("movieId") === firstDataframe("movieId"),"inner").drop(firstDataframe("movieId"))
      .sort(desc("sum(rating)"))

    //dataframeMovieRating.show()

    val nameOfRecommendedFilm = dataframeMovieRating.select("title").collect.map(_.getString(0)).take(150)
    //nameOfRecommendedFilm.foreach(println)

    val idOfRecommendedFilm = dataframeMovieRating.select("movieId").collect.map(_.getInt(0)).take(150)
    //idOfRecommendedFilm.foreach(println)


    (nameOfRecommendedFilm,idOfRecommendedFilm)

  }

  //FUNCTION USED TO RECOMMEND 20 RANDOM MOVIES
  def randomRecommender(oldDataset: RDD[Rating]): (Array[String],Array[Int]) = {
    import ss.implicits._

    val oldDat = oldDataset.toDF()
    //test.show(80)

    val data = ss.read.format("csv").option("header", "true").load("DatasetWithID/rating.csv")
    val movies = ss.read.format("csv").option("header","true").load("DatasetWithID/movie.csv")
    val movie = movies.withColumn("movieId", $"movieId".cast("Integer"))

    println("------------------------ RANDOM PREFERENCES ------------------------ ")

    var onlySelectedMovies = oldDat.where("user == 0")
    //onlySelectedMovies.show(30)

    val renamedOnlySelectedMovies = onlySelectedMovies.toDF("userId","movieId","rating")



    val dataframeMovieRating = movie.join(renamedOnlySelectedMovies,movie("movieId") === renamedOnlySelectedMovies("movieId"),"left_anti").drop(renamedOnlySelectedMovies("movieId"))

    print("\n")

    val shuffledDF = dataframeMovieRating.orderBy(rand())

    val nameOfRandomFilm = shuffledDF.select("title").collect.map(_.getString(0)).take(150)
    //nameOfRandomFilm.foreach(println)

    val idOfRandomFilm = shuffledDF.select("movieId").collect.map(_.getInt(0)).take(150)
    //idOfRandomFilm.foreach(println)


    (nameOfRandomFilm,idOfRandomFilm)

  }

  //ASK USER INPUT ABOUT RANDOM AND TOP RATED FILMS AND UPDATE THE MODEL
  def askUserInput(idAndName: (Array[String], Array[Int]),oldDataset: RDD[Rating]): RDD[Rating] = {
    import ss.implicits._

    val movieRecommended = idAndName._1
    val movieID = idAndName._2

    val movieNameArray = new Array[String](20)
    val movieIDArray = new Array[Int](20)

    val ratingArr = new Array[Double](20)
    println("Insert rating (pay attention, rating must be between 0 and 5)[If you don't know the movie, enter 6 to skip it]:"); print("\n")

    var iteration = 0
    var filmNumber = 0

    while(iteration <= 19) {
      print(movieRecommended(filmNumber))
      print(", how do you rate this film? ")
        Try(scala.io.StdIn.readLine().toInt).fold(_ =>
        {
          println("Error: this isn't a number")
          iteration
        }
          , n => if (n == 6) {
            print("\nGoing to next film\n\n");
            filmNumber = filmNumber + 1
            iteration
          }else if(n < 1 || n > 6) {
            println("Error: check the range")
            iteration
          }
          else {
            movieNameArray(iteration) = movieRecommended(filmNumber)
            movieIDArray(iteration) = movieID(filmNumber)
            ratingArr(iteration) = n
            filmNumber = filmNumber + 1
            iteration = iteration + 1
            n
          })
    }

    val userId = Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)


    val resultOfRating = sc.parallelize(ratingArr)
    val movieIdentification = sc.parallelize(movieIDArray)
    val userID = sc.parallelize(userId)


    val dataframeResultOfRating = resultOfRating.map(rate => rate).toDF("rating")
    //dataframeResultOfRating.show()
    val dataframeMovieID = movieIdentification.map(a => a).toDF("movieId")
    //dataframeMovieID.show()
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

    print("\n")

    print("Thank you for your feedback! \n \n")

    finalDataRDDRating

  }

  //ADDING THE NEW RATINGS TO THE DATASET
  def updateModel(oldDataset: RDD[Rating], newDataset: RDD[Rating]): RDD[Rating] = {

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


    //----------------------------------------------------------------------------
    //SET OF THE ALS ALGORITHM
    var actualAlsAlgorithm = ALSAlgo(results)


    //----------------------------------------------------------------------------
    //CALCULATION OF THE MEAN SQUARE ERROR
    val predictionsWithMapping = predictionWithMapping(results,actualAlsAlgorithm) // PREDICTIONS OF ALL USERS COMPARED TO THEIR RATES
    //predictionsWithMapping.foreach(println)

    val mSError = MSE(actualAlsAlgorithm,predictionsWithMapping)
    println("The mean square error of this model is: " + mSError)


    //----------------------------------------------------------------------------
    //MAIN MENU
    var actualDataset = results


    print("What you want to do?\n")
    print("- Press 0 to shut down the programm\n")
    print("- Press 1 to rate 20 top rated films\n")

    print("Selection: ")

    var signal = 0
    var counter = 3

    while(counter != 0) {
      if (signal == 0) { //CHECK IF USER ALREADY RATE TOP RATED FILMS
        val choose = scala.io.StdIn.readLine()
        Try(choose.toInt).toOption match {
          case Some(rate) => {
            if (rate == 0) { //IF INPUT IS 0 EXIT FROM WHILE AND FINISH PROCESS
              counter = 0
            }else if (rate == 1) { //IF INPUT IS 1 ASK USER RATE FOR TOP RATED FILMS
              val topRatedFilms = topRated()
              val newDatasetTopRated = askUserInput(topRatedFilms, results)
              val updatedDataset = updateModel(results, newDatasetTopRated)
              actualDataset = updatedDataset
              actualAlsAlgorithm = ALSAlgo(updatedDataset)
              signal = 1
            }else {
              print("Error: check range\n"); print("Selection: ")
            }
          }
          case _ =>
            println("Error: check type of your input"); print("Selection: ")
        }
      }else{ //IF USER DON'T RATE TOP RATED FILM THEN:
        print("What you want to do now?\n")
        print("- Press 0 to shut down the programm\n")
        print("- Press 2 to rate 20 random films\n")
        print("- Press 3 to see top rated films for you\n")
        val choose = scala.io.StdIn.readLine()
        Try(choose.toInt).toOption match {
          case Some(rate) => {
            if (rate == 0) { //EXIT FROM WHILE CYCLE
              counter = 0
            }else if (rate == 2) { //IF INPUT IS 2 ASK RATE FOR RANDOM MOVIES
              val randomFilms = randomRecommender(actualDataset)
              val newDatasetRandom = askUserInput(randomFilms, actualDataset)
              val updatedAgain = updateModel(actualDataset, newDatasetRandom)
              actualDataset = updatedAgain
              actualAlsAlgorithm = ALSAlgo(actualDataset)
            }else if(rate == 3){ //IF INPUT IS 3 PRINT RECOMMENDED MOVIES BASED ON PREVIOUS USER PREFERENCES
              println("------------------------ RECOMMENDED MOVIES ------------------------")
              println("20 recommended films for you: ")
              topRatedForKnownUser(actualAlsAlgorithm, 0, titles)
              print(" \n "); print(" \n ")
            }else {
              print("Error: check range\n")
              print("Selection: ")
            }
          }
          case _ =>
            println("Error: check type of your input")
            print("Selection: ")
        }
      }
    }
    print("Thank you, bye!")
  }




}

