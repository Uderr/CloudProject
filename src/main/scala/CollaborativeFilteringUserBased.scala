import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lit, monotonically_increasing_id, rand, sum}

import scala.util.{Random, Try}





object CollaborativeFilteringUserBased {

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

  val smallerData = sc.textFile("Dataset/test.csv")

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
  //FUNCTIONS USED TO CALCULATE PREDICTIONS BASED ON COSINE SIMILARITY

  //FUNCTION USED TO CALCULATE COSINE SIMILARITY OF USER USER 0 AND ALL OTHERS USERS
  def cosineSimilarity(results: DataFrame): Dataset[Row] = {

    import org.apache.spark.sql.functions._

    val firstResults = results
    var allUserAndRating = firstResults.join(results,firstResults("movieId") === results("movieId"), "full").distinct()
    allUserAndRating = allUserAndRating.toDF("userId1","movieId","rat1","userId2","movieId2","rat2").drop("movieId2")
    val removeSameUser = allUserAndRating.where("userId1 != userId2").orderBy("userId1","userId2")

    var cosSim = removeSameUser.withColumn(
      "rat1",    // normalize rat1
      coalesce(
        col("rat1") / sqrt(sum(col("rat1") * col("rat1")).over(Window.partitionBy("userId1"))),
        lit(0)
      )
    ).withColumn(
      "rat2",    // normalize rat2
      coalesce(
        col("rat2") / sqrt(sum(col("rat2") * col("rat2")).over(Window.partitionBy("userId2"))),
        lit(0)
      )
    ).withColumn(
      "rat1_times_rat2",
      col("rat1") * col("rat2")
    ).groupBy("userId1", "userId2").agg(sum("rat1_times_rat2").alias("cosineSim")).orderBy("userId1")



    cosSim = cosSim.where("userId1 ==" + 0).orderBy(desc("cosineSim"))



    cosSim
  }

  //FUNCTION USED TO CALCULATE PREDICTIONS ACCORDING TO COSINE SIMILARITY
  def calculateMovieBasedOnSimilarity(cosSim: Dataset[Row], oldData: RDD[Rating]): RDD[Rating] ={

    import ss.implicits._

    val oldDat = oldData.toDF("userId","movieId","rating")
    //cosSim.show()


    val movies = ss.read.format("csv").option("header","true").load("DatasetWithID/movie.csv")
    val movie = movies.withColumn("movieId", col("movieId").cast("Integer"))

    val onlySelectedMovies = oldDat.where("userId == 0")
    val renamedOnlySelectedMovies = onlySelectedMovies.toDF("userId","movieId","rating")
    val dataframeMovieRating = movie.join(renamedOnlySelectedMovies,movie("movieId") === renamedOnlySelectedMovies("movieId"),"left_anti")
      .drop(renamedOnlySelectedMovies("movieId"))

    dataframeMovieRating.where("movieId = 296").show()

    val tableWithDuplicateMovies = cosSim.join(oldDat, oldDat("userId") === cosSim("userId2"), "right")
      .drop(cosSim("userId1"))
      .drop(oldDat("userId"))
      .where(col("userId2").isNotNull)
    val completeTable = tableWithDuplicateMovies.join(dataframeMovieRating,tableWithDuplicateMovies("movieId") === dataframeMovieRating("movieId"),"inner")
      .drop("type")
      .drop(tableWithDuplicateMovies("movieId"))
      .toDF("userId","cosSim","rating","movieId","title")
      .withColumn("ratePred", col("rating")*col("cosSim"))
      .orderBy(desc("cosSim"),desc("rating"))
    //completeTable.show()

    val test = completeTable.join(completeTable.groupBy("movieId").sum("ratePred"), Seq("movieId"))
    //test.show()

    val tes2 = test.join(test.groupBy("movieId").sum("cosSim"), Seq("movieId"))
    //tes2.show()

    val test3 = tes2.withColumn("rating",col("sum(ratePred)") / col("sum(cosSim)"))
      .drop("cosSim","userId","ratePred","sum(ratePred)","sum(cosSim)").withColumn("userId",lit(0))
      .distinct()
      .orderBy("rating")
      .limit(20)

    val finalDataRDDRating = test3.select("userId", "movieId", "rating").rdd.map(r => Rating(r.getInt(0), r.getInt(1),r.getDouble(2)))


    val nameOfFilm = test3.select("title").collect().map(_.getString(0))

    val rateOfFilm = test3.select("rating").collect.map(_.getDouble(0))

    val counter = nameOfFilm.length
    print("\n \n According to the cosine similarity you may like the films: \n")
    for(i <- 0 to counter - 1){
      print(nameOfFilm(i) + " with an aproximate rate of " + rateOfFilm(i) + "\n")
    }
    print("\n \n")


    finalDataRDDRating

  }












  //----------------------------------------------------------------------------
  //MAIN FUNCTION

  def main(args: Array[String]): Unit = {


    //----------------------------------------------------------------------------
    //CREATION OF TUPLES
    val results = data.map(ratingCreation)
    val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
    val smallResults = smallerData.map(ratingCreation)


    //----------------------------------------------------------------------------
    //SET OF THE ALS ALGORITHM
    var actualAlsAlgorithm = ALSAlgo(results)


    //----------------------------------------------------------------------------
    //CALCULATION OF THE MEAN SQUARE ERROR
    val predictionsWithMapping = predictionWithMapping(results,actualAlsAlgorithm) // PREDICTIONS OF ALL USERS COMPARED TO THEIR RATES
    //predictionsWithMapping.foreach(println)

    val mSError = MSE(actualAlsAlgorithm,predictionsWithMapping)
    println("(ALS) The mean square error of this model is: " + mSError)


    //----------------------------------------------------------------------------
    //MAIN MENU

    //VARIABLES USED TO REMEMBER USER PREFERENCES
    var actualDataset = results
    var actualSmallerDataset = smallResults

    print("What you want to do?\n")
    print("- Press 0 to shut down the programm\n")
    print("- Press 1 to rate 20 top rated films\n")

    print("Selection: ")

    var signal = 0
    var counter = 3

    //MAIN MENU
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
              actualDataset = updateModel(actualDataset, newDatasetTopRated)
              actualAlsAlgorithm = ALSAlgo(actualDataset)
              actualSmallerDataset = updateModel(actualSmallerDataset, newDatasetTopRated)
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
        print("- Press 4 to see movies that you can like according to cosine similarity\n")
        val choose = scala.io.StdIn.readLine()
        Try(choose.toInt).toOption match {
          case Some(rate) => {
            if (rate == 0) { //EXIT FROM WHILE CYCLE
              counter = 0
            }else if (rate == 2) { //IF INPUT IS 2 ASK RATE FOR RANDOM MOVIES
              val randomFilms = randomRecommender(actualDataset)
              val newDatasetRandom = askUserInput(randomFilms, actualDataset)
              actualDataset = updateModel(actualDataset, newDatasetRandom)
              actualAlsAlgorithm = ALSAlgo(actualDataset)
            }else if(rate == 3) { //IF INPUT IS 3 PRINT RECOMMENDED MOVIES BASED ON PREVIOUS USER PREFERENCES
              println("------------------------ RECOMMENDED MOVIES ------------------------")
              println("20 recommended films for you: ")
              topRatedForKnownUser(actualAlsAlgorithm, 0, titles)
              print(" \n ");
              print(" \n ")
            }else if(rate == 4){ //IF INPUT IS 4 CALCULATE COSINE SIMILARITY AND RECOMMENDATIONS BASED ON
              val dataFromResults = ss.createDataFrame(actualSmallerDataset).toDF("userId","movieId","rating")
              val cosSim = cosineSimilarity(dataFromResults)
              val topRatedMovies = calculateMovieBasedOnSimilarity(cosSim,actualSmallerDataset)
              print("\nInsert 1 to add these prediction to your database: ")
              var yesOrNo = scala.io.StdIn.readInt()
              if(yesOrNo == 1){
                actualDataset = updateModel(actualDataset,topRatedMovies)
              }
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

