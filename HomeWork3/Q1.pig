users = LOAD  ‘/Fall2014_HW-3-Pig/users_new.dat' using PigStorage(‘#’)  AS (user_id:int,gender:chararray,age:int); 
ratings =  LOAD  ‘/Fall2014_HW-3-Pig/ratings_new.dat' using PigStorage(‘#’)  AS (user_id:int,movie_id:int,rating:int);
movies =  LOAD  ‘/Fall2014_HW-3-Pig/movies_new.dat' using PigStorage(‘#’)  AS (movie_id:int,title:chararray,genres:chararray);
Action_and_War_movies = FILTER movies by ((genres matches '.*War.*') AND (genres matches '.*Action.*')); 
Female_users =  FILTER users by ((gender matches 'F') AND (age>20 AND age<35));
movies_group = group ratings by movie_id;

movies_average = FOREACH  movies_group { GENERATE group AS movie_id,AVG(ratings.rating) AS rating;};

filtered_rated_movies = JOIN Action_and_War_movies  by movie_id,movies_average by movie_id;

X = FOREACH filtered_rated_movies GENERATE Action_and_War_movies::movie_id as movie_id,Action_and_War_movies::title as title,Action_and_War_movies::genres as Genres,movies_average::rating as rating;

Y = GROUP X ALL;
M = FOREACH Y GENERATE MAX(X.rating);
tups = Filter X by (rating==M.$0);

highest_rated_movies = JOIN ratings by movie_id,tups by movie_id;

RESULT = FOREACH highest_rated_movies  GENERATE  ratings::user_id as user_id,ratings::movie_id as movie_id,ratings::rating as rating,tups::title as title;

r3 = JOIN RESULT by user_id,Female_users by user_id;
final = FOREACH r3 GENERATE RESULT::user_id as user_id,Female_users::gender as gender,Female_users::age as age,RESULT::movie_id as movie_id,RESULT::title as title,RESULT::rating as rating;
describe final;
dump final;






