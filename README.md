## Using PIG to find the top rated OLDEST movie

The goal of this script is to find the oldest top rated movie in the IMDB data set. The intent of this project is to use another method
for extracting insights other than using MapReduce. In this case PIG is using TEZ as it's mapping/reducing agent to showcase a faster 
alternative to using MapReduce. 

# Script run down

First we'll load our data set into two separate pig data structures: 
ratings = LOAD '/usr/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/usr/maria_dev/ml-100k/u.item' USING PigStorage('|')
  AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

Note, the u.item data set is pipe delimeted and so we needed to alert Pig of this.

Next, we create a name look up table to ensure we actually know what the title of the movie is other than using its ID. 

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate,'dd-MMM-yyyy')) AS releaseTime;

We then group the ratings by movie ID - our first accumulation step if you will. 

ratingsByMovie = GROUP ratings BY movieID;

Then we calculate the average rating per movie group: 

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

We then filter on movies that have ratings greater than 4:

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

We further filter on movies that have actual data associated with it because some of the data is not present in the data set. 

fiveStarsWithData = JOIN fiveStarMovies BY movieID, namelookup BY movieID;

Lastly, we order all movies (w/ data) 

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;

Overall this script works and appears to be easier to develop than using the MapReduce approach. In scale, I'm not sure if Pig is the way
to go but, from a coders perspective, it's a lot easier if you can get away with this. Seems like Pig and other functional languages (R)
share the same analytical process and, in the end, works for me. 
~

