import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class WordSimi {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("SoGou Simi");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        JavaRDD<String> jrdd = sc.textFile(args[0]);
        //JavaRDD<String> jrdd = sc.textFile("D:\\IDEA\\IDEAproject\\MR_wordCount\\target\\new 1.txt");
        JavaRDD<String> jrdd1 = jrdd.map(x -> x.split("\t")[1]);
        JavaRDD<String> jrdd2 = jrdd1.map(x -> x.replace("]", "").replace("[", "").replace("  "," ").trim());
        JavaRDD<Row>  jrdd3=jrdd2.repartition(1000).map(x->RowFactory.create(Arrays.asList(x.split(" "))));
        List<Row> rdd = jrdd3.collect();
         System.out.println(rdd.get(0));
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> documentDF = sparkSession.createDataFrame(rdd, schema);
        documentDF.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK());
       // documentDF.show(10);
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("features")
                .setVectorSize(6)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(documentDF);
        Dataset<Row> result = model.transform(documentDF);
        result.show(10);
        //JavaRDD<Vector> data=result.javaRDD().map(x->(Vector)(x.get(1))).cache();
        KMeans kmeans=new KMeans().setK(20).setSeed(1L);
        KMeansModel model_kmean=kmeans.fit(result);
        System.out.println("cluster Center:");
        for(Vector center:model_kmean.clusterCenters()){
            System.out.println(" "+center);
        }
        double cost=model_kmean.computeCost(result);
        System.out.println(cost);
        model_kmean.save("/lc/SogouQ/word2vector1M/kmean_model/");
       // KMeansModel sameModel = KMeansModel.load(sc.sc(),"target/org/apache/spark/JavaKMeansExample/KMeansModel");
        sc.stop();
    }
}
