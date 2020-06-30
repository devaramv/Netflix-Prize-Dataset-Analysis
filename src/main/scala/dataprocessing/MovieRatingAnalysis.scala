package dataprocessing

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import properties.GetConfigurations

object MovieRatingAnalysis {
  val logger: Logger = Logger.getLogger(MovieRatingAnalysis.getClass)

  case class Movie(movieId: Int, year: Int, name: String)

  case class MovieRatings(user: String, rating: Int, reviewDate: String, movieId: Int)

  def main(args: Array[String]): Unit = {

    val configFilePath = args(0)
    GetConfigurations.loadProperties(configFilePath) // load properties into memory


    val R = GetConfigurations.getR // input variable identifies the movies that are rated by atleast R users
    val M = GetConfigurations.getM
    val U = GetConfigurations.getU

    val individual_MovieRatings_Path = GetConfigurations.getIndividual_MovieRatings_Path
    val consolidated_MovieRatings_Dataset_Path = GetConfigurations.getConsolidated_MovieRatings_Path
    val movie_Titles_Location = GetConfigurations.getMovie_titles_location
    val consolidated_MovieRating_Output_Filename = GetConfigurations.getConsolidated_Output_FileName
    val consolidated_MovieRating_File = consolidated_MovieRatings_Dataset_Path + consolidated_MovieRating_Output_Filename
    val final_Output_Location = GetConfigurations.getFinal_Output_Location

    /*
         Step1 : Run the Data Consolidator Process

     */

    DataConsolidator.fileMerge(individual_MovieRatings_Path
      , consolidated_MovieRatings_Dataset_Path
      , consolidated_MovieRating_Output_Filename)

    /*
      Spark Session is an entry point into the spark program.
     */

    val spark = SparkSession
      .builder()
      .appName("Netflix-Dataset-Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val moviesList = spark.read.textFile(movie_Titles_Location)
      .map(_.split(","))
      .map(x => Movie(x(0).trim().toInt,
        x(1).trim().replace("NULL", "1000").toInt,
        x(2).trim())).toDF().cache()

    val allMovie_Ratings_Dataset = spark.read.textFile(consolidated_MovieRating_File)
      .map(_.split(","))
      .map(x => MovieRatings(x(0).trim(),
        x(1).trim().toInt,
        x(2).trim(),
        x(3).trim().toInt)).toDF()


    val moviesRated_By_AtleastRUsers = allMovie_Ratings_Dataset
      .groupBy("movieId")
      .count()
      .filter(s"count >= $R").toDF()

    import org.apache.spark.sql.functions._

    val movies_Withrating_descOrder = moviesRated_By_AtleastRUsers
      .select("movieId")
      .join(allMovie_Ratings_Dataset, "movieId") //Inner equi-join with another DataFrame using the given column.
      .groupBy("movieId")
      .avg("rating")
      .join(moviesList, "movieId")
      .sort(desc("avg(rating)"))
      .cache()


    val top_M_Minus_One_Movies = movies_Withrating_descOrder.limit(M - 1)

    val top_M_Movies_Tie_Windowspec = Window.partitionBy("avg(rating)").orderBy("name")

    val top_Mth_Spot_Tie_Check_Movie = movies_Withrating_descOrder.except(top_M_Minus_One_Movies)
      .withColumn("row_number", row_number().over(top_M_Movies_Tie_Windowspec))
      .filter("row_number = 1").limit(1).select("movieId", "name", "avg(rating)")


    val top_M_Movies = top_M_Minus_One_Movies.select("movieId", "name", "avg(rating)")
      .union(top_Mth_Spot_Tie_Check_Movie)


    val numberOfDistinctTopMovies = top_M_Movies.distinct().count()


    /*top_M_Movies_Comprehensive_Dataset will comprise of only top movies along with
      other information includes user, rating, reveiw-date, etc..
    */
    val top_M_Movies_Comprehensive_Dataset = top_M_Movies.join(allMovie_Ratings_Dataset, "movieId").cache()


    /*
       usersRatedAllTopMovies data frame provides the list of users
       that rated all the top_M_movies found above.

       Assuming one user rates one movie, must have count greater than or
       equal to top_M_Movies when group by each user.

     */


    val users_Rated_AllTopMMovies = top_M_Movies_Comprehensive_Dataset.groupBy("user")
      .count().where(s"count>=$numberOfDistinctTopMovies ").toDF()

    /*
         top_U_Users are the users who have given the lowest average
         rating of M movies.

         1. Find users have rated all top movies
         2. from above users, for each user find the average rating of all movies given by the user
         3. sort by avg(rating) in ascending order to get the lowest rated users of all top movies
         4. Pick the top U-1
         5. Apply tie braker for the Uth position using the lower ids when there is a tie for
         the position.


     */

    val top_Users_Averageratings = users_Rated_AllTopMMovies.join(top_M_Movies_Comprehensive_Dataset, "user")
      .groupBy("user")
      .avg("rating")
      .sort(asc("avg(rating)"))
      .toDF()
      .cache()

    val top_U_Minus_One_Users = top_Users_Averageratings.limit(U - 1)

    val top_U_Users_Tie_Windowspec = Window.partitionBy("avg(rating)").orderBy("user")

    val top_Uth_Spot_Tie_Check_User = top_Users_Averageratings.except(top_U_Minus_One_Users)
      .withColumn("row_number", row_number().over(top_U_Users_Tie_Windowspec))
      .filter("row_number = 1").limit(1).select("user", "avg(rating)")

    val top_U_Users = top_U_Minus_One_Users.union(top_Uth_Spot_Tie_Check_User)


    /*
      From the top_U_Users found above, find the highest ranked movie by each user
       1. Find the max rating given by the each contrarian user for the movie.
       2. Using the above result, find all the movies that the user has given the max rating
       3. There is a high chances of each user giving the same max rating to multiple movies. In such
       scenario apply tie braker. For each tie spot
         a. prefere recent publication (latest movie release year)
         b. if the above also ties then alphabetical order of the title in ascending order

     */

    val maxMovie_Rating_By_ContrarianUser = top_U_Users.select("user")
      .join(allMovie_Ratings_Dataset, "user")
      .groupBy("user")
      .max("rating").toDF()


    val mostLiked_Contrarian_User_Movies = maxMovie_Rating_By_ContrarianUser
      .join(allMovie_Ratings_Dataset, maxMovie_Rating_By_ContrarianUser("user") === allMovie_Ratings_Dataset("user") &&
        maxMovie_Rating_By_ContrarianUser("max(rating)") === allMovie_Ratings_Dataset("rating")).toDF()
      .join(moviesList, "movieId").toDF("movieID",
      "contrarianuser", "max(rating)", "user", "rating", "reviewDate", "year", "name") // toDF() is to avoid ambiguity
      .select("contrarianuser", "name", "year", "reviewDate", "movieId", "rating")

    mostLiked_Contrarian_User_Movies.cache() /* mostLiked_Contrarian_User_Movies is cached because it will be
     used multiple times while eliminating user rated movies based on Tie braker criteria */


    //unique_Users_List are the list of users that does not participate in the tie braker
    val unique_Users_List = mostLiked_Contrarian_User_Movies.groupBy("contrarianuser")
      .count().where("count=1").select("contrarianuser").toDF()

    // unique_Users_MovieList identfies movie information such as movieId,Name,Year and Data of rating
    val unique_Users_MovieList = unique_Users_List.join(mostLiked_Contrarian_User_Movies, "contrarianuser")


    /* Non_Unique_UserList are the list of users who need tie braker i.e. they have rated more than one movie with the
    same max rating */
    val Non_Unique_UserList = mostLiked_Contrarian_User_Movies.select("contrarianuser")
      .except(unique_Users_List.select("contrarianuser"))

    /*
        First preference is publication year then alphabetical order.
        Following set of next few data frames does the same

     */

    val contraianUsers_GroupByYear_Count = Non_Unique_UserList
      .join(mostLiked_Contrarian_User_Movies, "contrarianuser")
      .groupBy("contrarianuser", "year").count()

    val contraianUsers_WithMax_Year = Non_Unique_UserList
      .join(mostLiked_Contrarian_User_Movies, "contrarianuser")
      .groupBy("contrarianuser")
      .max("year")

    /*
      The following view is the consolidated view of contrarian users max year
      along with max(year) count. This is to identify users whos
      year of publication is highest and also to identify
      years who also tie in the year of publicaiton.
      +--------------+---------+--------------+----+-----+
|concontrarianuser|max(year)|contrarianuser|year|count|
+--------------+---------+--------------+----+-----+
|        164845|     1997|        164845|1997|    2|
|        727242|     1997|        727242|1997|    2|
|        786312|     2003|        786312|2003|    1|
|       1903324|     1997|       1903324|1997|    2|
|        595870|     1997|        595870|1997|    2|
|       1024854|     1997|       1024854|1997|    2|
|       1511683|     1997|       1511683|1997|    2|
|       1272379|     1997|       1272379|1997|    2|
|       2646115|     1997|       2646115|1997|    2|
|       1314869|     1997|       1314869|1997|    2|
|        603277|     1997|        603277|1997|    2|
|        873713|     1997|        873713|1997|    2|
|       2439493|     1997|       2439493|1997|    2|
+--------------+---------+--------------+----+-----+

     */

    val contrarianUsers_with_MaxYearView = contraianUsers_WithMax_Year
      .join(contraianUsers_GroupByYear_Count,
        contraianUsers_GroupByYear_Count("contrarianuser") === contraianUsers_GroupByYear_Count("contrarianuser")
          && contraianUsers_WithMax_Year("max(year)") === contraianUsers_GroupByYear_Count("year"))
      .toDF("maxyearviewcontrarianuser", "max(year)", "contrarianuser", "year", "maxyearcount")
      .select("maxyearviewcontrarianuser", "max(year)", "maxyearcount")
      .cache()


    val contrarianUsers_Movies_Year_Tie =
      contrarianUsers_with_MaxYearView.select("maxyearviewcontrarianuser", "max(year)")
        .filter("count==1")
        .join(mostLiked_Contrarian_User_Movies, mostLiked_Contrarian_User_Movies("contrarianuser") ===
          contrarianUsers_with_MaxYearView("maxyearviewcontrarianuser") && mostLiked_Contrarian_User_Movies("year")
          === contrarianUsers_with_MaxYearView("max(year)"))
        .select("maxyearviewcontrarianuser", "name", "year", "reviewDate", "movieId", "rating")


    val contrarianUsers_Movies_Alphabetical_Tie_Windowing = Window.partitionBy("contrarianuser").orderBy("name")

    val contrarianUserswithAlphabeticalOrder =
      contrarianUsers_with_MaxYearView.select("maxyearviewcontrarianuser")
        .filter("maxyearcount>1")
        .toDF("contrarianuser")
        .join(mostLiked_Contrarian_User_Movies, "contrarianuser")
        .sort(asc("contrarianuser"), asc("name"))
        .withColumn("row_number",
          row_number().over(contrarianUsers_Movies_Alphabetical_Tie_Windowing)).filter("row_number=1")
        .select("contrarianuser", "name", "year", "reviewDate", "movieId", "rating")

    val filteredContrarianMovies = unique_Users_MovieList.union(contrarianUsers_Movies_Year_Tie)
      .union(contrarianUserswithAlphabeticalOrder)
      .select("contrarianuser", "name", "year", "reviewDate")


    filteredContrarianMovies
      .write
      .csv(final_Output_Location)


  }
}
