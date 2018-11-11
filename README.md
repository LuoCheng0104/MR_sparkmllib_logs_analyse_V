# MR_sparkmllib_logs_analyse
Based on hadoop cluster, MapReduce is used to realize user search keyword aggregation, and the aggregated results are saved by spark. Word2vec algorithm is used to transform keywords into vectors and vectors as features, and Kmeans is adopted to conduct clustering analysis on users.
version:

hadoop 2.7.4

spark 2.1.0

kafka 2.11-2.0.0

Mapreduce Program submission method:

hadoop jar /home/bd/lc/jar/MR_wordCount-2.0_spark.jar MR_Sougou "hdfs://10.199.160.171:9000/lc/SogouQ/a*" "hdfs://10.199.160.171:9000/lc/SogouQ/outcome"

Spark Program submission method:

spark-submit --master yarn-client --driver-memory 4g --driver-cores 1 --executor-memory 3g --total-executor-cores 4 --class WordSimi /hom    e/bd/lc/jar/MR_wordCount-3.0_spark_kmeans.jar /lc/SogouQ/outcome/p1M
