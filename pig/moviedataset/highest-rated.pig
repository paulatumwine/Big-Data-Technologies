register '/usr/lib/pig/piggybank.jar';
movies1 = load 'input/movies.csv' as (line:chararray);
commaReplaced = foreach movies1 generate REPLACE (line, ',(?!(([^\\"]*\\"){2})*[^\\"]*$)', '');
quotesReplaced = foreach commaReplaced generate REPLACE($0,'"','') as record;
movies = foreach quotesReplaced generate 
	SUBSTRING($0, 0, INDEXOF($0, ',', 0)) as movieId:chararray,
	SUBSTRING($0, INDEXOF($0, ',', 0) + 1, LAST_INDEX_OF($0, ',')) as title:chararray,
	SUBSTRING($0, LAST_INDEX_OF($0, ',') + 1, (int) SIZE($0)) as genres:chararray
;
adventure = filter movies BY genres matches '.*Adventure.*';

ratings = load 'input/rating.txt' AS (userId:chararray, movieId:chararray, rating:int, timestamp:chararray);

joined = join adventure by movieId, ratings by movieId;
highestRated = filter joined by ratings::rating == 5;

projected = foreach highestRated generate (int)adventure::movieId as movieId:int, 'Adventure', ratings::rating, adventure::title;
unique = distinct projected;
sorted = order unique by movieId asc;
top20 = limit sorted 20;

store top20 INTO 'highest-rated-output';
