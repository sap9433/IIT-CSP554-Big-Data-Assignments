INSERT OVERWRITE TABLE MyDb.foodratingspart 
PARTITION (name)
SELECT food1, food2, food3, food4, id, name
FROM MyDb.foodratings;


SELECT FP.place, avg(FR.food4) AS AVG
FROM MyDb.foodplaces FP inner join MyDb.foodratings FR
on (FP.id = FR.id)
GROUP BY FP.place
having  FP.place = 'Soup Bowl'
