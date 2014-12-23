Users_data = LOAD ‘/Fall2014_HW-3-Pig/users_new.dat’ using PigStorage(‘#’) as (user_id:int,gender:chararray,age:int,zip:int);
Ratings_data = LOAD ‘/Fall2014_HW-3-Pig/ratings_new.dat’ using PigStorage(‘#’) as (user_id:int,movie_id:int,rating:float);
Grouped_data = COGROUP Users_data by user_id, Ratings_data by user_id;
Result = LIMIT Grouped_data 11;
describe Result;
dump Result;

