# MR_sparkmllib_logs_analyse1
Based on hadoop cluster, MapReduce is used to realize user search keyword aggregation, and the aggregated results are saved by spark. Word2vec algorithm is used to transform keywords into vectors and vectors as features, and Kmeans is adopted to conduct clustering analysis on users.2018/11/17 add kdtree algorithm to find nearby groups of users

version:

hadoop 2.7.4

spark 2.1.0

kafka 2.11-2.0.0

dataSets:
http://www.sogou.com/labs/resource/q.php

Data processing process:
data————>MapReduce——>word2vec————>Kmeans————>Kdtree
 
To be continued!!!!!!!!
