from pyspark.sql.types import *
from pyspark.sql.functions import *
spark.conf.set("spark.sql.crossJoin.enabled", "true")

movies_metadata_schema = StructType(
        [
                StructField("adult",BooleanType(), True),
                StructField("belongs_to_collection",StringType(), True),
                StructField("budget",DoubleType(), True),
                StructField("genres",StringType(), True),
                StructField("homepage",StringType(), True),
                StructField("id",IntegerType(), True),
                StructField("imdb_id",StringType(), True),
                StructField("original_language",StringType(), True),
                StructField("original_title",StringType(), True),
                StructField("overview",StringType(), True),
                StructField("popularity",DoubleType(), True),
                StructField("poster_path",StringType(), True),
                StructField("production_companies",StringType(), True),
                StructField("production_countries",StringType(), True),
                StructField("release_date",StringType(), True),
                StructField("revenue",StringType(), True),
                StructField("runtime",StringType(), True),
                StructField("spoken_languages",StringType(), True),
                StructField("status",StringType(), True),
                StructField("tagline",StringType(), True),
                StructField("title",StringType(), True),
                StructField("video",BooleanType(), True),
                StructField("vote_average",StringType(), True),
                StructField("vote_count",StringType(), True)
        ]
)

imdb_movie_data_schema = StructType(
        [
                StructField("Rank",IntegerType(), True),
                StructField("Title",StringType(), True),
                StructField("Genre",StringType(), True),
                StructField("Description",StringType(), True),
                StructField("Director",StringType(), True),
                StructField("Actors",StringType(), True),
                StructField("Year",IntegerType(), True),
                StructField("Runtime",DoubleType(), True),
                StructField("Rating",DoubleType(), True),
                StructField("Votes",IntegerType(), True),
                StructField("Revenue",DoubleType(), True),
                StructField("Metascore",IntegerType(), True)
        ]
)


movies_metadata = spark.read.schema(movies_metadata_schema).csv('/user/maria_dev/final_project/movies_metadata.csv')
movies_metadata.withColumn("release_date", unix_timestamp(col("release_date"), "yyyy-MM-dd").cast("timestamp"))
movies_metadata.withColumn("runtime", col("runtime").cast("double"))
movies_metadata.withColumn("revenue", col("revenue").cast("double"))
movies_metadata.withColumn("vote_average", col("vote_average").cast("double"))
movies_metadata.withColumn("vote_count", col("vote_count").cast("double"))
imdb_movie_data = spark.read.schema(imdb_movie_data_schema).csv('/user/maria_dev/final_project/IMDB-Movie-Data.csv')

movies_metadata.printSchema()
print movies_metadata.head(2)

imdb_movie_data.printSchema()
print imdb_movie_data.head(2)

movies_metadata.createOrReplaceTempView("movies_metadata_T")
imdb_movie_data.createOrReplaceTempView("imdb_movie_data_T")

movies_metadata_count = spark.sql("SELECT COUNT(*) FROM movies_metadata_T")
print ("movies_metadata_count table =>",movies_metadata_count.collect())

imdb_movie_data_count = spark.sql("SELECT COUNT(*) FROM imdb_movie_data_T")
print ("imdb_movie_data_count table =>",imdb_movie_data_count.collect())

join_count = spark.sql("SELECT COUNT(*) FROM imdb_movie_data_T imdb, movies_metadata_T movies WHERE lower(imdb.Title) = lower(movies.original_title)")
print ("join_count =>",join_count.collect())

upd_join_count = spark.sql("SELECT COUNT(*) FROM imdb_movie_data_T imdb, movies_metadata_T movies WHERE lower(imdb.Title) = lower(movies.original_title) AND (imdb.Year = year(movies.release_date) OR imdb.Runtime = movies.runtime)")
print ("upd_join_count =>",upd_join_count.collect())

target_table_title_year = spark.sql("SELECT movies.imdb_id AS Imdb_id, coalesce(imdb.Genre,movies.genres) AS Genre,	movies.original_title AS Title,	coalesce(movies.overview,imdb.Description) AS Description, imdb.Director AS Director, movies.release_date AS Release_date, imdb.Year AS Release_Year, case when (movies.revenue is not null and imdb.Revenue is not null) Then (movies.revenue + (imdb.Revenue * 1000000))/2 when movies.revenue is null Then imdb.Revenue ELSE movies.revenue End AS Revenue, case when (movies.runtime is not null and imdb.Runtime is not null) Then (movies.runtime + imdb.Runtime)/2 when movies.runtime is null Then imdb.Runtime ELSE movies.runtime End AS Runtime, case when (movies.vote_average is not null and imdb.Rating is not null) Then (movies.vote_average + imdb.Rating)/2 when movies.vote_average is null Then imdb.Rating ELSE movies.vote_average End AS Ratings, case when (movies.vote_count is not null and imdb.Votes is not null) Then (movies.vote_count + imdb.Votes)/2 when movies.vote_count is null Then imdb.Votes	ELSE movies.vote_count End AS Votes FROM imdb_movie_data_T imdb, movies_metadata_T movies WHERE lower(imdb.Title) = lower(movies.original_title) AND imdb.Year = year(movies.release_date)") 
print ("target_table_title_year record =>",target_table_title_year.head(1))

imdb_movie_unmatched = spark.sql("SELECT * FROM imdb_movie_data_T WHERE Title NOT IN (SELECT imdb.Title FROM imdb_movie_data_T imdb, movies_metadata_T movies WHERE imdb.Title = movies.original_title AND imdb.Year = year(movies.release_date))")
imdb_movie_unmatched.createOrReplaceTempView("imdb_movie_unmatched_T")

imdb_movie_unmatched_count = spark.sql("SELECT COUNT(*) FROM imdb_movie_unmatched_T")
print ("imdb_movie_unmatched_count =>",imdb_movie_unmatched_count.collect())

upd_join_count = spark.sql("SELECT COUNT(*) FROM imdb_movie_unmatched_T imdb, movies_metadata_T movies WHERE imdb.Title = movies.original_title AND imdb.Runtime = movies.runtime")
print ("upd_join_count =>",upd_join_count.head(2))

target_table_title_runtime = spark.sql("SELECT movies.imdb_id AS Imdb_id, coalesce(imdb.Genre,movies.genres) AS Genre,	movies.original_title AS Title,	coalesce(movies.overview,imdb.Description) AS Description, imdb.Director AS Director, movies.release_date AS Release_date, imdb.Year AS Release_Year, case when (movies.revenue is not null and imdb.Revenue is not null) Then (movies.revenue + (imdb.Revenue * 1000000))/2 when movies.revenue is null Then imdb.Revenue ELSE movies.revenue End AS Revenue, case when (movies.runtime is not null and imdb.Runtime is not null) Then (movies.runtime + imdb.Runtime)/2 when movies.runtime is null Then imdb.Runtime ELSE movies.runtime End AS Runtime, case when (movies.vote_average is not null and imdb.Rating is not null) Then (movies.vote_average + imdb.Rating)/2 when movies.vote_average is null Then imdb.Rating ELSE movies.vote_average End AS Ratings, case when (movies.vote_count is not null and imdb.Votes is not null) Then (movies.vote_count + imdb.Votes)/2 when movies.vote_count is null Then imdb.Votes	ELSE movies.vote_count End AS Votes FROM imdb_movie_unmatched_T imdb, movies_metadata_T movies WHERE lower(imdb.Title) = lower(movies.original_title) AND imdb.Runtime = movies.runtime")
print ("target_table_title_runtime record =>",target_table_title_runtime.head(1))

target_table = target_table_title_year.unionAll(target_table_title_runtime)
target_table.printSchema()
print target_table.head(2)
print target_table.count()

target_table.write.mode('overwrite').format('orc').saveAsTable("IMDB_Movies")
target_table.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save('/user/maria_dev/final_project/target_table')