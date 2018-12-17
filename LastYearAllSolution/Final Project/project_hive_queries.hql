-------Describe Hive table-------------
describe formatted IMDB_Movies;

-------movie count each release_year-------------
select release_year, count(*) as Movies from IMDB_Movies group by release_year order by Movies DESC;

-------Movie count each director directed---------------------
select Director, count(*) as Movies from IMDB_Movies group by Director HAVING Movies > 4 order by Movies DESC;

--------movie count each year with rating--------------------------------
select release_year, ratings, count(*) as Movies from IMDB_Movies group by release_year, ratings HAVING Movies > 4 order by Movies DESC;

--------Revenue Stylized Facts per year---------------
select release_year, sum(Revenue) as Sum_Revenue, avg(Revenue) as Avg_Revenue, min(Revenue) as Min_Revenue, max(Revenue) as Max_Revenue from IMDB_Movies group by release_year order by release_year DESC;

--------Avg. Revenue of movies per director---------------
select Director, Avg(Revenue) as Avg_Revenue from IMDB_Movies group by Director HAVING Avg_Revenue > 100000 order by Avg_Revenue DESC;

--------Ratings Stylized Facts per year---------------
select release_year, sum(ratings) as Sum_Rating, avg(ratings) as Avg_Rating, min(ratings) as Min_Rating, max(ratings) as Max_Rating from IMDB_Movies group by release_year order by release_year DESC;