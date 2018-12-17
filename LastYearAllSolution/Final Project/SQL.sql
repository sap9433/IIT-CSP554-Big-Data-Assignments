--  target_table_title_year =
-----------------------------------------------------------------------------------------
SELECT movies.imdb_id AS Imdb_id, 
	coalesce(imdb.Genre,movies.genres) AS Genre, 
	movies.original_title AS Title, 
	coalesce(movies.overview, imdb.Description) AS Description, 
	movies.release_date AS Release_date, 
	case 
		when (movies.revenue is not null and imdb.Revenue is not null) 
			Then (movies.revenue + (imdb.Revenue * 1000000))/2
		when movies.revenue is null 
			Then imdb.Revenue 
		ELSE movies.revenue 
	End AS Revenue, 
	case 
		when (movies.runtime is not null and imdb.Runtime is not null) 
			Then (movies.runtime + imdb.Runtime)/2
		when movies.runtime is null 
			Then imdb.Runtime 
		ELSE movies.runtime 
	End AS Runtime,
	case 
		when (movies.vote_average is not null and imdb.Rating is not null) 
			Then (movies.vote_average + imdb.Rating)/2
		when movies.vote_average is null 
			Then imdb.Rating 
		ELSE movies.vote_average 
	End AS Ratings,
	case 
		when (movies.vote_count is not null and imdb.Votes is not null) 
			Then (movies.vote_count + imdb.Votes)/2
		when movies.vote_count is null 
			Then imdb.Votes 
		ELSE movies.vote_count 
	End AS Votes,
FROM imdb_movie_data_T imdb, movies_metadata_T movies 
	WHERE imdb.Title = movies.original_title AND imdb.Year = year(movies.release_date)
	
	
----------------------------------------------------------------------------------------------------------------------
-- target_table_title_runtime =
----------------------------------------------------------------------------------------------------------------------
SELECT movies.imdb_id AS Imdb_id, 
	coalesce(imdb.Genre,movies.genres) AS Genre, 
	movies.original_title AS Title, 
	coalesce(movies.overview, imdb.Description) AS Description, 
	movies.release_date AS Release_date, 
	case 
		when (movies.revenue is not null and imdb.Revenue is not null) 
			Then (movies.revenue + (imdb.Revenue * 1000000))/2
		when movies.revenue is null 
			Then imdb.Revenue 
		ELSE movies.revenue 
	End AS Revenue, 
	case 
		when (movies.runtime is not null and imdb.Runtime is not null) 
			Then (movies.runtime + imdb.Runtime)/2
		when movies.runtime is null 
			Then imdb.Runtime 
		ELSE movies.runtime 
	End AS Runtime,
	case 
		when (movies.vote_average is not null and imdb.Rating is not null) 
			Then (movies.vote_average + imdb.Rating)/2
		when movies.vote_average is null 
			Then imdb.Rating 
		ELSE movies.vote_average 
	End AS Ratings,
	case 
		when (movies.vote_count is not null and imdb.Votes is not null) 
			Then (movies.vote_count + imdb.Votes)/2
		when movies.vote_count is null 
			Then imdb.Votes 
		ELSE movies.vote_count 
	End AS Votes, 
FROM imdb_movie_unmatched_T imdb, movies_metadata_T movies 
	WHERE imdb.Title = movies.original_title AND imdb.Runtime = movies.runtime