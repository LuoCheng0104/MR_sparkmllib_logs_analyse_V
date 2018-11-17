import edu.wlu.cs.levy.CG.KeyDuplicateException;
import edu.wlu.cs.levy.CG.KeySizeException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.Arrays;
import java.util.List;

/**
 * The program is used to conveniently find the neighbor group of the target user,
 * input the keyword and neighbor number N,and output the neighbor users
 */

public class FindNeighbor {
    public static void main(String[] args) throws KeySizeException, KeyDuplicateException, AnalysisException {
        if (args.length < 4) {
            System.err.print("Usage:<# test file path> <# N neighbor>");
            System.err.println(" <# word2vec model path> <# Kmeans model path>");
            System.exit(1);
        }
        String testfilePath=args[0];
        int N_neighbor=Integer.parseInt(args[1]);
        String word2vec_model_path=args[2];
        String cluster_model_path=args[3];
        SparkConf conf = new SparkConf().setAppName("SoGou Simi").setMaster("local[*]");
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        JavaRDD<String> test_jrdd = sc.textFile(testfilePath);
        JavaRDD<String> test_jrdd2 = test_jrdd.map(x -> StringProcess.testFileProcess(x));//关键词以空格分隔
        JavaRDD<Row>  test_jrdd3=test_jrdd2.map(x->(RowFactory.create(Arrays.asList(x.split("\t")[0]),
                                                 Arrays.asList(x.split("\t")[1].split(" ")))));
        List<Row> rdd_test= test_jrdd3.collect();
        StructType schema = new StructType(new StructField[]{
                new StructField("user_id", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> documentDF = sparkSession.createDataFrame(rdd_test, schema);//test file to DataFrame
        Word2VecModel word2Vec_model=Word2VecModel.load(word2vec_model_path);//load word2Vec model
        Dataset<Row> word2Vec_test_result=word2Vec_model.transform(documentDF);
        KMeansModel kMeansModel_model=KMeansModel.load(cluster_model_path);//load kMeansModel model
        Dataset<Row> kmean_test_result=kMeansModel_model.transform(word2Vec_test_result);
        kmean_test_result.createTempView("kmean_test_result");
        Dataset<Row> test=sparkSession.sql("select user_id,features, prediction from kmean_test_result");
        test.show();
        JavaRDD<String> searchfile=sc.textFile("D:\\IDEA\\IDEAproject\\MR_wordCount\\data\\search_file\\p*");
        BulidTreeAndSearch bts=new BulidTreeAndSearch(test,searchfile);
        List<List<String>> re=bts.find(N_neighbor);//得到最近邻的N个用户ID
        for(List<String> s:re){
            System.out.println(s);
        }
        sc.stop();
    }
}
