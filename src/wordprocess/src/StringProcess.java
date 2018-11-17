import org.apache.spark.sql.Row;

public class StringProcess {
    /**�ı��ļ��ؼ����Կո�ָ�*
     * for example:
     * 0	  [����] [���﷿��]  [����ֻ���λ���������]  [��װ�]->
     * 0	���� ���﷿�� ����ֻ���λ��������� ��װ�
     */
    public static String testFileProcess(String x){
        return x.split("\t")[0]+"\t"+x.split("\t")[1].
                replace("]", "").replace("[", "").
                replace("  "," ").trim();
    }
    /**�ַ���תdouble���飺��ʽ��[element1,element2,...]*/
    public static double[] String2DoubleArray(String str){
        String[] arr=str.replace("]","").replace("[","").split(",");
        double[] key=new double[arr.length];
        for(int i=0;i<arr.length;i++){
            key[i]=Double.parseDouble(arr[i]);
        }
        return key;
    }
    /**Rowת�ַ���*/
    public static String Row2String(Row row){
        return row.get(0).toString().replace("WrappedArray(","")
                .replace(")","")+" "+row.get(1).toString().
                replace("WrappedArray(","").replace(")","");
    }
    /**�ַ���ת��
     * example��[WrappedArray(user_id),[Vector],prediction]
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
