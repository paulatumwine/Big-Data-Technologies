movies = load 'input/movies.csv' as (line:chararray);

/*
replace comma(,) if it appears in column content
replace the quotes("") which is present around the column if it have comma(,) as its a csv file feature
idea taken from https://stackoverflow.com/a/45662346/2732708
*/
commaReplaced = foreach movies generate REPLACE (line, ',(?!(([^\\"]*\\"){2})*[^\\"]*$)', '');
quotesReplaced = foreach commaReplaced generate REPLACE($0,'"','') as record;

-- STORE quotesReplaced INTO 'tmpOutput' USING PigStorage(',');
-- movies1 = load 'tmpOutput/part-m-00000' using PigStorage(',') as (movieId:chararray, title:chararray, genres:chararray);

movies1 = foreach quotesReplaced generate 
	SUBSTRING($0, 0, INDEXOF($0, ',', 0)) as movieId:chararray,
	SUBSTRING($0, INDEXOF($0, ',', 0) + 1, LAST_INDEX_OF($0, ',')) as title:chararray,
	SUBSTRING($0, LAST_INDEX_OF($0, ',') + 1, (int) SIZE($0)) as genres:chararray
;

filtered = filter movies1 by STARTSWITH(title, 'A') or STARTSWITH(title, 'a');
genres = foreach filtered generate genres;
tokenizedGenres = foreach genres generate TOKENIZE(genres, '|'); 
flatTokens = foreach tokenizedGenres generate flatten($0);

grouped = group flatTokens by $0;
counts = foreach grouped generate group, COUNT(flatTokens);

store counts into 'movies-by-genre-output';
