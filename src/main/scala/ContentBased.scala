import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ContentBased {


  //SET OF SPARK ENVIRONMENT
  val conf = new SparkConf().setAppName("Cont-Based").setMaster("local[*]")
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
  val ratings = ss.read.format("csv").option("header","true").load("s3://cloudprogramming/DatasetWithID/rating.csv")
  val movies = ss.read.format("csv").option("header","true").load("s3://cloudprogramming/DatasetWithID/movie.csv")

  def main(args: Array[String]): Unit = {

    val userNumber = 5  //numero di utenti
    val recommendationsNumber = 20  //numero di raccomandazioni di film per gli utenti
    var results = "---\n\n"
    var t0: Long = 0
    var t1: Long = 0
    t0 = System.nanoTime() //tempo iniziale
    results = printReccomendation("Raccomandazioni Basate sul Contenuto", contentBasedRecommendations(ratings, movies), recommendationsNumber, userNumber)
    t1 = System.nanoTime() //tempo finale

    //calcolo il tempo impiegato per l'elaborazione
    //tempo finale meno il tempo iniziale (prima e dopo l'esecuzione)
    results += "tempo impiegato: " + ((t1-t0)/Math.pow(10,9)) + "s\n\n"

    println(results)

  }

  //gestisco la stampa finale delle raccomandazioni per gli utenti
  def printReccomendation(recommenderSystemType: String, recommendationArray: RDD[(Int, Seq[(String, Float)])], recommendationsNumber: Int, numberUsers: Int): String = {
    var strToPrint = recommenderSystemType + "\n"
    //ordino l'array delle raccomandazioni per il numero di utenti (crescente da 1 a 5)
    val arr = recommendationArray.takeOrdered(numberUsers)(Ordering[Int].on(x => x._1)) //x._1: colonna utente. Restituisce array ordinato
      .map(tuple => (tuple._1, tuple._2.sortWith(_._2 > _._2).take(recommendationsNumber)))
    //due cicli for: il primo per gli utenti; il secondo per le raccomandazioni
    for (i <- arr.indices) {
      strToPrint += "utente: " + arr(i)._1 + " -> "
      for (j <- arr(i)._2.indices) {
        //indice       //titolo                        //rating
        strToPrint += "\t" + (j+1) + ")'" + arr(i)._2(j)._1 + "' score: " + arr(i)._2(j)._2 + "/5"
        if (j < arr(i)._2.length-1)
          strToPrint += ";"
      }
      strToPrint += "\n"
    }
    strToPrint+"\n"
  }

  def contentBasedRecommendations(rdf: DataFrame, mdf: DataFrame): RDD[(Int, Seq[(String, Float)])] = {
    //per il filtraggio basato sul contenuto, vengono presi in considerazione solo 5 rating con punteggio di 3 o più
    val ratingConsidered = selectMostMovieRated(mdf, rdf)

    //la matrice delle informazioni sul genere G, è una matrice mxk con m film e k generi.
    // 1 se il film appartiene al genere, 0 altrimenti
    val movieGenresMatrix = buildMoviesGenresMatrix(mdf)

    //il risultato del prodotto scalare tra la matrice di rating e la matrice di genere,
    //è una matrice n×k detta P, che contiene la predisposizione di ciascun utente verso ciascun genere
    val predispositionUserGenre = calculatePredispositionUserGenre(ratingConsidered, movieGenresMatrix)

    //calcolo dei film già visti per ogni utente
    val userAlreadySeenMovies = calculateAlreadySeenMovies(rdf)
    val reccomendationWithMovieId = calculateContendBasedRecommendation(predispositionUserGenre, movieGenresMatrix, userAlreadySeenMovies)
    val recommendationWithMovieTitle = findMovieTitleForRecommendation(mdf, reccomendationWithMovieId)

    //print(selectMostMovieRated(movies, ratings).getClass) //class org.apache.spark.rdd.MapPartitionsRDD
    //print(buildMoviesGenresMatrix(movies).getClass) //class scala.collection.immutable.Map$WithDefault
    //print(calculatePredispositionUserGenre(ratingConsidered, movieGenresMatrix).getClass) //class org.apache.spark.rdd.MapPartitionsRDD
    //print(calculateAlreadySeenMovies(ratings).getClass) //class scala.collection.immutable.Map$WithDefault
    //recommendationWithMovieTitle.foreach(println)

    recommendationWithMovieTitle
  }

  def selectMostMovieRated(movieDf: DataFrame, ratingDf: DataFrame, numberMostRatedCons: Int = 5) : RDD[(Int, Seq[(Int, Float)])] = {
    val idMovieWithoutGenres =
      movieDf
        //0: movieId, 1: genres
        .select("movieId", "genres")
        .rdd.filter(r => r.getString(1) == "(Nessun genere elencato)")
        //movieId convertiti in Int
        .map(_.getString(0).toInt)
        .collect()

    ratingDf
      //seleziono solo i film, di cui conosco il genere, con il rating più rilevante (da 3 o più)
      .filter(ratingDf("rating").cast(IntegerType) >= 3 && !ratingDf("movieId").cast(IntegerType).isin(idMovieWithoutGenres: _*))
      .rdd
      .map(row => {
        val userId = row.getString(0).toInt
        val movieId = row.getString(1).toInt
        val rating = row.getString(2).toFloat
        val timestamp = row.getString(3).toInt
        (userId, (movieId, rating, timestamp))
      })
      //raggruppo per ogni utente
      .groupByKey()
      .map(element => (
        //userId come chiave dell'rdd in output
        element._1,
        element._2.toSeq
          //ordino in base al timestamp
          .sortBy(_._3)(Ordering[Int].reverse)
          //rating rilevanti
          .take(numberMostRatedCons)
          //rimuovo il timestap perché non necessario
          .map(triple => (triple._1, triple._2))))
  }

  //la matrice delle informazioni sul genere G, è una matrice mxk con m film e k generi.
  //1 se il film è di quel genere, 0 altrimenti.
  def buildMoviesGenresMatrix(moviesDf: DataFrame, arrayOfGenres: Array[String] = Array("Action", "Adventure", "Animation",
    "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX", "Musical", "Mystery",
    "Romance", "Sci-Fi", "Thriller", "War", "Western")) : Map[Int, Array[Float]] = {
    moviesDf
      .select("movieId", "genres")
      .rdd
      //rimuovo i film che non hanno informazioni sul genere (0: movieId - 1: genres)
      .filter(row => row.getString(1) != "(no genres listed)")
      .map(row => {
        val filmId = row.getString(0).toInt
        val currentMovieGenresArray = row.getString(1).split('|')
        //inizializza la riga della matrice per ogni film
        val currentMovieGenresRow = Array.fill(arrayOfGenres.length)(0.toFloat)
        //aggiorno la riga della matrice con il corrente genere del film
        currentMovieGenresArray.foreach(genre => {
          val i = arrayOfGenres.indexOf(genre)
          currentMovieGenresRow(i) = 1.toFloat
        })
        //rdd che rappresenta la matrice di generi
        (filmId, currentMovieGenresRow)
      })
      .collect()
      .toMap.withDefaultValue(Array.fill(arrayOfGenres.length)(0.toFloat))
  }

  //il risultato del prodotto scalare tra la matrice di rating e la matrice di genere,
  //è una matrice n×k detta P, che contiene la predisposizione di ciascun utente verso ciascun genere
  def calculatePredispositionUserGenre(ratingConsidered: RDD[(Int, Seq[(Int, Float)])], genreMatrix: Map[Int, Array[Float]]): RDD[(Int, Array[Float])] = {
    ratingConsidered
      //row: userId, List((movieId, rating), ....)
      .map(row => (
        row._1,
        //Dalla matrice dei generi viene selezionata la riga corrispondente al film votato e si moltiplica ogni valore con il rating corrispondente.
        row._2.map(singleRating => genreMatrix(singleRating._1).map(_ * singleRating._2))))
      //somma per ogni utente tutti gli array di predispesizione del genere ottenuti
      .map(userInformation => ( userInformation._1, userInformation._2.reduce((array1,array2) => array1.zip(array2).map { case (x, y) => x + y })))
  }

  //calcola i film già visti per ogni utente
  def calculateAlreadySeenMovies(ratingDf: DataFrame): Map[Int, Set[Int]] = {
    ratingDf
      .select("userId", "movieId")
      .rdd
      .map(r => {
        val userId = r.getString(0).toInt
        val movieId = r.getString(1).toInt
        (userId, movieId)
      })
      //ragruppo per ogni utente
      .groupByKey()
      .map(element => (element._1, element._2.toSet))
      .collect()
      .toMap.withDefaultValue(Set())
  }

  //SULLA BASE DI QUESTA MATRICE DI 'PREDISPOSIZIONE' DETTA P, È POSSIBILE FORMULARE RACCOMANDAZIONI BASATE SUL CONTENUTO PER UN UTENTE
  //CALCOLANDO LA 'COSINE SIMILARITY' TRA IL VETTORE DEL PROFILO UTENTE (RIGA u-esima DELLA MATRICE P PER L'UTENTE u)
  //E LA MATRICE DELLE INFORMAZIONI SUL GENERE G

  //calcolo la somiglianza tra 2 vettori di uguale lunghezza
  def cosineSimilarity(arrayP: Array[Float], arrayG: Array[Float]): Float = {
    if (arrayP.length != arrayG.length)
      throw new Exception("Error in Cosine Similarity: Arrays with Different Lengths")
    //similarità del coseno
    val numRes = (arrayP zip arrayG).map(c => c._1 * c._2).sum
    val denomRes = math.sqrt(arrayP.map(math.pow(_, 2)).sum) * math.sqrt(arrayG.map(math.pow(_, 2)).sum)
    var cosRes = numRes
    if (denomRes != 0)
      cosRes /= denomRes.toFloat
    cosRes
  }

  def calculateContendBasedRecommendation(predispositionUserGenre: RDD[(Int, Array[Float])], movieGenresMatrix: Map[Int, Array[Float]], usersAlreadySeenMovies: Map[Int, Set[Int]]) : RDD[(Int, Seq[(Int, Float)])] = {
    (for{
      userPredispositionArray <- predispositionUserGenre
      //non considero i film già visti
      //ottengo un insieme di film già visti per l'utente corrente
      setAlreadySeenMoviesCurrentUser = usersAlreadySeenMovies(userPredispositionArray._1)
      //nella matrice dei generi, considero solo i film non visti
      movieGenresArray <- movieGenresMatrix.filter(movieInformation => !setAlreadySeenMoviesCurrentUser.contains(movieInformation._1)).toSeq
    }
    //ritorno userId, (movieId, recommendationValue) //BISOGNA RICHIAMARE 'cosineSimilarity'
      yield (userPredispositionArray._1, (movieGenresArray._1, cosineSimilarity(userPredispositionArray._2, movieGenresArray._2)))
      )
      //ragruppo dati per ogni utente
      .groupByKey()
      //elenco di raccomandazioni
      .map(userRecommendation => {
        val user = userRecommendation._1
        val movieScores = userRecommendation._2.toSeq.map(t => (t._1, t._2*5)).sortBy(_._2)(Ordering[Float].reverse)
        (user, movieScores)
      })
  }

  def findMovieTitleForRecommendation(moviesDf: DataFrame, recommendationWithMovieId: RDD[(Int, Seq[(Int, Float)])]) : RDD[(Int, Seq[(String, Float)])] = {
    //calcolo film e titoli
    val moviesAndTitles = moviesDf.select("movieId", "title").rdd.map(r => (r(0).toString.toInt, r(1).toString))
    //dato un 'movieId', restituisco il titolo del film corrispondente
    val moviesMap = moviesAndTitles.collect().toMap
    recommendationWithMovieId.map(row => (row._1, row._2.map(movieRecommendation => (moviesMap(movieRecommendation._1), movieRecommendation._2))))
  }


}


