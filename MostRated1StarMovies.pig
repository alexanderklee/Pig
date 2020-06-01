## Load ratings and some helper metadata into local data structures
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime: int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (omvieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

## Iterate through metadata and get movie information that will minimize the amount of data we
## really need to work with
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;


## We then group ratings by the movie
groupRatings = GROUP ratings BY movieID; 

## We then iterate through and collect all movie ratings, averages and the number of of times a
## a movie was rated
averageRatings = FOREACH groupRatings GENERATE group as movieID,
	AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS numRatings;

## We further filter down move list by removing any movies with ratings greater than 1
badMovies = FILTER averageRatings BY avgRating < 2.0;

## We join all 1-star movies into a list
namedBadMovies = JOIN badMovies BY movieID, nameLookup by movieID;

## collect all movies with low ratings
finalResults = FOREACH namedBadMovies GENERATE nameLookup::movieTitle AS movieName,
	badMovies::avgRating AS avgRating, badMovies::numRatings AS numRatings; 

## Sorty low rated movies by ratings
finalResultsSorted = ORDER finalResults BY numRatings DESC;

DUMP finalResultsSorted; 

