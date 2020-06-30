## Analysis of Netflix Prize Dataset

**Goal:** *Looking for the movies that are well loved by users who dislikes movies most users like*

**Requirements**: 

Using the input data set 

1. Find the top_M_Movies. These are the movies which are rated highest across all users
2. top_M_Movies have to be found from the movies which have been rated highest by at least R users
3. of users which have rated top_M_Movies, find U users which have given the lowest average rating of M movies
4. For the U contrarian users, find each users highest ranked movie.
5. Final output will have userID, Title of the highest rated movie by Contrarian users, Year of release of movie, Date of rating

**My Approach:**

Using the consolidated dataset

**Finding the top_M_Movies:**

1. Find the movies grouping by movie Id having count >=R which gives movies rated by at least R users
2. From the above result set, using the group by movie, avg(rating) then sort by descending order for picking up the movies that are highest rated across all users. In this step, I picked to M-1 Users
3. Calculating the tie for the Mth spot. Filter all the users that are not part of the M-1 users then partition by rating and order by movie name using row_number(window function). This allows the movie that comes first in the alphabetical order to get picked in the list in the case of tie
4. Pick the top one movie from the above result set.
5. Join M-1 users data set with the MTh movie data set to get the top_M_Movies

**Finding the top_U_Users:**

1. From the result set of top_M_Movies,do the following
2. Find the users that have rated all top movies
3. From the above users, for each user find the average rating of all movies given by the user
4. Sort by avg(rating) in ascending order to get the lowest users of all top movies
5. Pick top U-1
6. Apply tie braker similar to top_M_Movies for the Uth position using the lower ids when there is a tie for the rating. 
7.  Join U-1 with Uth user to get the top_U_Users .

**Finding the contrarian users' most liked movies:**

1. From the top_U_users results set, find the movies that are highest rated by the U users.
2. For the tie break, I considered three scenarios includes unique users without tie, users movies considered  based on  year, and users movies that will be finalized by movies alphabetical order.
3. All three were calculated individually and union  to get the final dataset.

##### Technologies used

1. JDK 1.8
2. Scala 2.11.12
3. Apache Spark 2.3.2
4. Gradle 5.2.1

#### Input Dataset Description

1. Input dataset has *17770 files* in the **training_set** directory. Each file corresponds to a movie with 

  movie Id  separated by : being the first line.

  Rest of the lines in the file are comma separated values of user- id,rating,review date.

Ex: Top few lines from movie file 1

```
1:
 1488844,3,2005-09-06
 822109,5,2005-05-13
 885013,4,2005-10-19
 30878,4,2005-12-26
 823519,3,2004-05-03
 893988,3,2005-11-17
 124105,4,2004-08-05
 1248029,3,2004-04-22
 1842128,4,2004-05-09
```

2. *movie_titles.txt* contains each movies information in a comma separated format as follows

   movie id, year, name  

#### **Design**

There are two components in this project

	1. Data Cleansing
 	2. Data Analysis

##### Data Cleansing: 

As all the movies information in the input dataset is scattered between several files, uniquely identifying a movie for all its metadata information including user, rating is not possible which is an important aspect of the Data Analysis. Because of this a process will run to clean the data, create the necessary dataset that can be used as the input for the Analysis. This process does the following operations.

```git
Data Consolidation process will run through each file and does the following operations
1. Read the first line
2. split by ":" to extract the moviedId
3. append the movieId to the end of every line to make it ready for data analysis
```

Java method used for data consolidation signature is provided below

```java
DataConsolidator.Java
    
public static void fileMerge(String inputPath, String outputPath, String filename) throws IOException

inputPath :location of input movie files
outPath: location of output consolidated dataset of all movie files
filename : names of the file will be used while storing the output of the consolidated dataset of all movies.
    
```

After running this process,output looks like shown below and includes all movie ratings information along with user id.

```java
1488844,3,2005-09-06,1
822109,5,2005-05-13,1
885013,4,2005-10-19,1
30878,4,2005-12-26,1
823519,3,2004-05-03,1
893988,3,2005-11-17,1
124105,4,2004-08-05,1
1248029,3,2004-04-22,1
1842128,4,2004-05-09,1
```



##### Data Analysis:

Data Analysis is performed by **Apache Spark**. Spark uses the output produced in the previous step i.e. data consolidation to conduct the required analysis.  **Spark SQL, DataFrames API** libraries are used for the implementation.

```
1. Read datasets in to sparks memory
2. Assign schema for each data set (movieList, consolidated movie rating dataset) in spark using case classes
3. Applied necessary spark transformations to get Top_M_Movies,Top_U_Users and finally list of movies liked by the Top_U_Users

```



**Why Spark?**

Spark SQL, DataFramesAPI facilitates structured data processing. 

DataFrameAPI provides a way to organize data in the form of rows and columns efficiently in memory from files, RDD's, etc.. that can be joined against other data frames just like relational databases for performing the aggregate operations.

Query execution plans are optimized by the spark engine  using Spark SQL Catalyst optimizer

Majority of the operations performed as part of analysis requires intermediate outputs as the input for the next operation. Spark provides better ways to handle such scenarios through in memory processing computation engine, providing the ability to cache intermediate results in the memory, etc.



#### Installation:

```
Download project-repository
cd into project-directory
gradle clean
gradle build 
```



#### How to run:

**Step 1:** Create a configuration file MovieRatingAnalysis.properties that looks like below. Naming convention is optional. Please do not change the variable names, just update the values

```nginx
#inputConfigs of Users

R=10
M=3
U=5


#Data Set I/O paths

#input path of N movieFiles
individual_MovieRatings_Path=/u/username/training_set
consolidated_Output_FileName=allMovieDataset

#output path of consolidated N movieFiles where output will be stored after cleansing process

consolidated_MovieRatings_Path=/u/username/consolidatedoutput/

#input path where data file with movie_titles is stored
movie_titles_Location=/u/user/movie_titles_Location

final_Output_Location=/u/username/Rubicon_TakeHomeAssignment_Output.csv


```



**Step 2:**  Process can be run either in local or cluster. Following steps guide to run in local mode

1. Navigate to the project repo

2. Open the project in IDE (IntellijIdea Recommended)

3. Add **.master("local[*]")** to SparkSession. 

4. After step 3, SparkSession looks like this 

   ```
   val spark = SparkSession
     .builder()
     .appName("Netflix-Dataset-Analysis")
     .master("local[*]")
     .getOrCreate()
   ```

5. Pass the **MovieRatingAnalysis.properties** created above as an argument

6. Hit run or press Shift+F10 to trigger the job

7. Output will be stored in the **final_Output_Location** specified in the configuration file

**Step 3:**  This step helps to run the process in cluster mode

1. Navigate to the project repo
2. Do gradle build then copy the jar to the cluster where spark is installed
3. Deploy using spark-submit



**Final Output:** 

Output results for the Run-1 with the input config is provided below. Part files that generated as a result of the run and the consolidated text file output of the run can be found in the projects directory.

```Java
Run-1 :

R=50
M=5
U=25

Output:
2232563,Hotel Rwanda,2005,2005-05-02
305344,Sports Illustrated Swimsuit Edition: 2005,2005,2005-02-12
1227322,Hotel Rwanda,2005,2005-11-14
2140883,12 Angry Men,1957,2002-10-29
525666,12 Angry Men,1957,2003-12-17
387418,2 Fast 2 Furious,2003,2005-06-26
1555581,12 Monkeys,1995,2004-08-12
1114324,24: Season 3,2003,2004-12-08
2238663,3-2-1 Penguins: Runaway Pride at Lightstation Kilowatt,2002,2005-05-21
 
Note: Output files related to Related to the Run-1 can be found at  Run-1_Output
    

```







