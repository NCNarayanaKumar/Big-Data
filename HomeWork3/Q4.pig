REGISTER /home/004/r/rx/rxs136530/FORMAT_GENRE.jar; 
Movies_data = LOAD ‘/Fall2014_HW-3-Pig/movies_new.dat’ using PigStorage(‘#’) as (movie_id:int,title:chararray,genres:chararray);
Result = FOREACH Movies_data GENERATE movies_id,title,FORMAT_GENRE(genres);
describe Result;
dump Result;

