import edu.wlu.cs.levy.CG.KDTree;
import edu.wlu.cs.levy.CG.KeyDuplicateException;
import edu.wlu.cs.levy.CG.KeySizeException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;
/**
 * This program is used to send the aggregated results of MR_Sougou. Main
 * into sparkmllib to train the Word2Vec model and Kmeans model,
 * and save the model and the training results
 */
public class WordSimi {
    public static void main(String[] args) throws IOException, AnalysisException, KeySizeException, KeyDuplicateException {
        SparkConf conf = new SparkConf().setAppName("SoGou Simi").setMaster("local[*]");
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        JavaRDD<String> jrdd = sc.textFile(args[0]);
        JavaRDD<String> jrdd2 = jrdd.map(x -> StringProcess.testFileProcess(x));
        JavaRDD<Row>  jrdd3=jrdd2.map(x->(RowFactory.create(Arrays.asList(x.split("\t")[0]),Arrays.asList(x.split("\t")[1].split(" ")))));
        List<Row> rdd = jrdd3.collect();
        StructType schema = new StructType(new StructField[]{
                new StructField("user_id", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> documentDF = sparkSession.createDataFrame(rdd, schema);
        //documentDF.repartition(1).persist(StorageLevel.MEMORY_AND_DISK());
       // Word2Vec将搜索的关键字转换成向量
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("features")
                .setVectorSize(6)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(documentDF);
        model.save("D:\\IDEA\\IDEAproject\\MR_wordCount\\src\\modelfile\\Word2VecModel");
        Dataset<Row> result = model.transform(documentDF);
        //将向量作为特征进行聚类
        KMeans kmeans=new KMeans().setK(3).setSeed(1L);
        KMeansModel model_kmean=kmeans.fit(result);
        model_kmean.save("D:\\IDEA\\IDEAproject\\MR_wordCount\\src\\modelfile\\KMeansModel");
        Dataset<Row> prediction=model_kmean.transform(result);
        prediction.createTempView("results_table");
        prediction.show();
        sparkSession.sql("select prediction from results_table").show();
        Dataset<Row> fea=sparkSession.sql("select max(user_id),features, max(prediction) from results_table group by features");
        fea.toJavaRDD().map(x->StringProcess.Row2String(x)+" "+x.get(2).toString()).saveAsTextFile("D:\\IDEA\\IDEAproject\\MR_wordCount\\data\\search_file");//save the search file for FindNeighbor.main args
        JavaRDD<String> feas=fea.toJavaRDD().map(new Function<Row,String>() {
            @Override
            public String call(Row row) throws Exception {
                return StringProcess.Row2String(row);
            }
        }
        );
        JavaPairRDD<String,String> jpr=feas.mapToPair(x->new Tuple2<String,String>(x.split(" ")[0], x.split(" ")[1]));
        List<Tuple2<String,String>> list=jpr.collect();
        KDTree<String> kd_tree=new KDTree<String>(6);
        for(Tuple2<String,String> s:list){
            kd_tree.insert(StringProcess.String2DoubleArray(s._2),s._1);
        }
        //test Vector[0,0,0,0,0,0]
        double[] test={0,0,0,0,0,0};
        List<String> ner=kd_tree.nearest(test,10);
        System.out.println(ner);
        sc.stop();
    }
}
