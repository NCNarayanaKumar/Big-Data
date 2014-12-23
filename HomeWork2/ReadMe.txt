

For Part a

1) hdfs dfs -rmr hdfs://localhost:9000/output

2) hadoop jar /Users/surapathiramesh/BigDataProjects/HomeWork3/MapSideJoin.jar MapSideJoin /input/users.dat /input/ratings.dat /output 1

For part b

1) hdfs dfs -rmr hdfs://localhost:9000/output1
hdfs dfs -rmr hdfs://localhost:9000/output2
hdfs dfs -rmr hdfs://localhost:9000/output3

2) hadoop jar /Users/surapathiramesh/BigDataProjects/HomeWork3/MapSideJoin.jar ReduceSideJoin /input/ratings.dat /output1 /input/movies.dat /output2 /output3
