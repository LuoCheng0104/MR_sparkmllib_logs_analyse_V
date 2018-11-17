import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * This program is used to do user search keywords aggregation,
 * and the aggregated files into HDFS
 */
public class MR_Sougou {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text userid = new Text();
        private Text words = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // StringTokenizer itr = new StringTokenizer(value.toString());
            String[] text=value.toString().split("\n");
            for(String line:text){
                userid.set(line.split("\t")[0]);
                words.set(line.split("\t")[1]);
                context.write(userid,words);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException {
            String words_list ="";
            HashSet<String> set=new HashSet<String>();
            for (Text val : values) {
                if(!set.contains(val.toString())) {
                    set.add(val.toString());
                    words_list = words_list + " " + val.toString();
                }
            }
            result.set(words_list);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MR_Sougou");
        job.setJarByClass(MR_Sougou.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);

    }

}

