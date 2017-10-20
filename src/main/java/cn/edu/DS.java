package cn.edu;
import java.io.IOException;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 *
 * 固定路径的wordCount
 * 直接在eclipse中运行
 *
 */

public class DS {

    //main函数，创建job对象：
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
       // FileUtil.deleteDir("output1");
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "out/artifacts/MapReduceDemo_jar/MapReduceDemo.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //conf.set("mapred.job.tracker","hdfs://172.31.238.103:8032");
        //conf.set("fs.default.name","hdfs://172.31.238.103:8020");
        String[] otherArgs = new String[]{"input/dream.txt", "output4"};
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "DS");
        job.setJarByClass(DS.class);
        job.setMapperClass(DS.MyMapper.class);
        job.setReducerClass(DS.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        protected void map(LongWritable k1,Text v1,Context context)throws IOException,InterruptedException{
            String[] splited = v1.toString().split(" ");
            for(String word : splited){
                context.write(new Text(word), new LongWritable(1));
            }
        };
    }

    static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        protected void reduce(Text k2,Iterable<LongWritable> v2,Context context) throws IOException, InterruptedException{
            long times = 0L;
            for (LongWritable count: v2){
                times += count.get();
            }
            context.write(k2, new LongWritable(times));
        };
    }
}
