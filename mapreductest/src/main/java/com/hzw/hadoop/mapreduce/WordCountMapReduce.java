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
 * @Description: 标准写法
 */
public class WordCountMapReduce extends Configured implements Tool {
  /**一行一行将文本数据读取到内存中
   * <0,hadoop yarn>
   * <1,hdfs yarn>
   * <2,java yarn>
   *   map程序获取到一行数据hadoop yarn 根据分隔符空格拆分为
   *   <hadoop,1>
   *   <yarn,1>
   */
  //step1: map 分组group，将相同key的value合并在一起，放到一个集合中
  public static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text mapOutputKey = new Text();

    private final static IntWritable mapOutputValue = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // value 是空格分割的字符串   hadoop yern hdfs java python
      String lineValue = value.toString();
//      String[] s = lineValue.split(" ");
      StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
      while (stringTokenizer.hasMoreTokens()){
        String wordValue = stringTokenizer.nextToken();
        mapOutputKey.set(wordValue);
        context.write(mapOutputKey,mapOutputValue);
      }

    }
  }

  /**
   * 在shuffle 中分组group，将相同key的value合并放在一起，就是将map的结果，<hadoop,1><yarn,1><hadoop,1><hadoop,1>，shuffle的=处理为<hadoop,list(1,1,1)>
   */

  //step2: reduce
  public static class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

    private  IntWritable outputValue = new IntWritable(1);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable value : values) {
        sum += value.get();
      }

      outputValue.set(sum);

      context.write(key,outputValue);

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
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    //3.3 reduce
    job.setReducerClass(WordCountReduce.class);
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

    int status = ToolRunner.run(configuration, new WordCountMapReduce(), args);

  }

}
