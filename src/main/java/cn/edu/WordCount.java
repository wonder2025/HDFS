        package cn.edu;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.conf.Configured;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        import org.apache.hadoop.util.Tool;
        import org.apache.hadoop.util.ToolRunner;

        import java.io.IOException;


 // Created by
public class WordCount extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        try {
            Configuration conf = getConf();
            conf.set("mapreduce.job.jar", "out/artifacts/HDFS_jar/HDFS.jar");
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            //conf.set("mapred.job.tracker","hdfs://172.31.238.103:8031");
            //conf.set("fs.default.name","hdfs://172.31.238.103:8020");
            //配置作业
            Job job = Job.getInstance(conf,"wc_demo");
            job.setJarByClass(WordCount.class);
            //HDFS上，分布式
            Path job_output = new Path("/user/scdx03/output");
//            Path job_output = new Path("/user/tony/output5");
            //序列化，通信用的字节流，要变成对象
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setMapperClass(WcMapper.class);
            job.setReducerClass(WcReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

//            FileInputFormat.setInputPaths(job, new Path("/user/tony/input/dream.txt"));
            FileInputFormat.setInputPaths(job, new Path("/user/scdx03/input/dream.txt"));
            job_output.getFileSystem(conf).delete(job_output, true);
            FileOutputFormat.setOutputPath(job, job_output);

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static class WcMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String mVal = value.toString();
            String[] temp=mVal.split(" ");
            for (String st:temp) {
                context.write(new Text(st), new LongWritable(1));
            }
        }
    }
    public static class WcReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable lVal : values){
                sum += lVal.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        //intellij连接hadoop平台的插件
        System.setProperty("hadoop.home.dir", "E:\\hadoop");
        ToolRunner.run(new WordCount(), args);

    }
}