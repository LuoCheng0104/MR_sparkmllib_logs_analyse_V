import org.apache.spark.sql.Row;

public class StringProcess {
    /**文本文件关键词以空格分隔*
     * for example:
     * 0	  [翻译] [莆田房产]  [免费手机定位软件下载网]  [异装癖]->
     * 0	翻译 莆田房产 免费手机定位软件下载网 异装癖
     */
    public static String testFileProcess(String x){
        return x.split("\t")[0]+"\t"+x.split("\t")[1].
                replace("]", "").replace("[", "").
                replace("  "," ").trim();
    }
    /**字符串转double数组：格式：[element1,element2,...]*/
    public static double[] String2DoubleArray(String str){
        String[] arr=str.replace("]","").replace("[","").split(",");
        double[] key=new double[arr.length];
        for(int i=0;i<arr.length;i++){
            key[i]=Double.parseDouble(arr[i]);
        }
        return key;
    }
    /**Row转字符串*/
    public static String Row2String(Row row){
        return row.get(0).toString().replace("WrappedArray(","")
                .replace(")","")+" "+row.get(1).toString().
                replace("WrappedArray(","").replace(")","");
    }
    /**字符串转换
     * example：[WrappedArray(user_id),[Vector],prediction]
     * ->user_id+" "+[vector]+" "+prediction
     */
    public static String Stringtransfrom(String str){
        String s= str.split("\\),")[0].replace("[WrappedArray(","").
                replace(")","")+" "+str.split("\\),")[1].
                replace("WrappedArray(","").replace(")","").
                replace("]]","]");
        String s1= s.split("],")[0]+" "+s.split("],")[1].replace("]","");
        return s1.split(" ")[0]+" "+s1.split(" ")[1]+"] "+s1.split(" ")[2];

    }
}
