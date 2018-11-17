import edu.wlu.cs.levy.CG.KDTree;
import edu.wlu.cs.levy.CG.KeyDuplicateException;
import edu.wlu.cs.levy.CG.KeySizeException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;
import java.util.List;
/**
 *This program is used to call the KdTree program,
 * the establishment of search tree, neighbor search
*/
 public class BulidTreeAndSearch {
     private static Dataset<Row> testFile;
     private static JavaRDD<String> searchFile;
    /**
     * Creates BulidTreeAndSearch
     * @param testFile testfile
     * @param searchFile  seachFile
     */
     public BulidTreeAndSearch(Dataset<Row> testFile, JavaRDD<String> searchFile){
         this.testFile=testFile;
         this.searchFile=searchFile;
     }
    /**
     * Creates a KD-tree with specified number of dimensions.
     * @param cluster_num number of cluster center num
     */
     public  KDTree<String> BulidTree(int cluster_num) throws KeySizeException, KeyDuplicateException {
         //int cluster_num = Integer.parseInt(line.split(" ")[2]);
         JavaRDD<String> seacrh = this.searchFile.filter(x ->Integer.parseInt(x.split(" ")[2]) == cluster_num);
         //JavaRDD<String> search_1 = seacrh.map(x -> StringProcess.Stringtransfrom(x.toString()));
         List<String> list = seacrh.collect();
         int size = StringProcess.String2DoubleArray(list.get(1).split(" ")[1]).length;//get KD-tree with specified number of dimensions
         KDTree<String> kd_tree = new KDTree<String>(size);
         for (String s : list) {
             String[] s1 = s.split(" ");//[user_id,features,prediction]
             kd_tree.insert(StringProcess.String2DoubleArray(s1[1]), s1[0]);
         }
         return kd_tree;
     }
    /**
     * start to look up neighbors with KD-tree .
     * @param N number of neighbors
     */
        public List<List<String>> find(int N) throws KeySizeException, KeyDuplicateException {
            ArrayList<List<String>> all_result = new ArrayList<List<String>>();
            JavaRDD<String> test=testFile.toJavaRDD().map(x->StringProcess.Row2String(x)+" "+x.get(2));
            test.take(0);
            List<String> test_list=test.collect();
            for(String t:test_list){
                List<String> result;
                KDTree<String> kd=null;
                kd=BulidTree(Integer.parseInt(t.split(" ")[2]));
                result=kd.nearest(StringProcess.String2DoubleArray(t.split(" ")[1]),N);
                result.add("Ä¿±êID:"+t.split(" ")[0]);
                //System.out.println(result);
                all_result.add(result);
            }
            return all_result;
        }


}
