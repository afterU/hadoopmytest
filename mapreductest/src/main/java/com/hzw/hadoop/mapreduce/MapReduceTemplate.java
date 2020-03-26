package com.hzw.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hzw
 * @date 2020/3/1  9:42 PM
 * @Description: mapreduce模版
 */
public class MapReduceTemplate extends Configured implements Tool {


  //step1: map 分组group，将相同key的value合并在一起，放到一个集合中
  //ToDo 定义类型
  public static class TemplateMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      //进行map操作之前的一些操作，例如读入数据
      //TODO
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      //ToDO 实现map逻辑
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      //map操作之后的一些操作，释放资源
    }
  }

  //step2: reduce
  //ToDO 定义类型
  public static class TemplateReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      //reduce前的一些操作
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      //TODO 实现reduce逻辑
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      //reduce之后的一些操作

    }
  }

  //step3: Driver
  public  int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    //1, configuration
    Configuration configuration = getConf();
    //2, 创建job
    Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());
    //3, set job
    // input -> map -> reduce -> output
    // 3.1 input
    Path inPath = new Path(args[0]);
    FileInputFormat.addInputPath(job,inPath);
    //3.2 map
    job.setMapperClass(TemplateMapper.class);
    //TODO 修改类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    //3.3 reduce
    job.setReducerClass(TemplateReduce.class);
    //TODO 修改类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //3.4 output
    Path outPath = new Path(args[1]);
    FileOutputFormat.setOutputPath(job,outPath);

    //4, submit job, true代表打印输出日期
    boolean status = job.waitForCompletion(true);

    return status ? 0 : 1;
  }

  public static void main(String[] args)
      throws Exception {

    Configuration configuration = new Configuration();

    int status = ToolRunner.run(configuration, new MapReduceTemplate(), args);

  }

}
