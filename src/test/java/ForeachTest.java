import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class ForeachTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("SoGou Simi").setMaster("local[*]");
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = sc.parallelize(data,1);
        int[] a=new int[data.size()];
        int j=0;
        List<Integer> A=javaRDD.collect();
        for(Integer i: A) {
            a[j++]=i;
        }
     System.out.println(Arrays.toString(a));
    }
}
